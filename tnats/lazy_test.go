package tnats

import (
	"context"
	"errors"
	"net"
	"runtime"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/xerr"
)

func startLazyTestServer(t *testing.T) string {
	t.Helper()
	server, err := natsserver.NewServer(&natsserver.Options{
		Host:      "127.0.0.1",
		Port:      natsserver.RANDOM_PORT,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		t.Fatalf("create embedded NATS server: %v", err)
	}
	go server.Start()
	if !server.ReadyForConnections(5 * time.Second) {
		server.Shutdown()
		server.WaitForShutdown()
		t.Fatal("embedded NATS server startup timed out")
	}
	t.Cleanup(func() {
		server.Shutdown()
		server.WaitForShutdown()
	})
	return server.ClientURL()
}

func TestCloseSerializesWithLazyConnectionSetup(t *testing.T) {
	url := startLazyTestServer(t)
	optionEntered := make(chan struct{})
	releaseOption := make(chan struct{})
	tr := New(url, WithNATSOptions(func(*nats.Options) error {
		close(optionEntered)
		<-releaseOption
		return nil
	}))

	connectDone := make(chan error, 1)
	go func() {
		_, err := tr.EnsureConn(t.Context())
		connectDone <- err
	}()
	select {
	case <-optionEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("NATS connection option was not reached")
	}

	closeStarted := make(chan struct{})
	closeDone := make(chan error, 1)
	go func() {
		close(closeStarted)
		closeDone <- tr.Close()
	}()
	<-closeStarted
	closingDeadline := time.After(2 * time.Second)
	for {
		tr.mu.RLock()
		closing := tr.closing
		tr.mu.RUnlock()
		if closing {
			break
		}
		select {
		case <-closingDeadline:
			t.Fatal("Close did not begin while connection setup was blocked")
		default:
			runtime.Gosched()
		}
	}
	close(releaseOption)

	select {
	case err := <-connectDone:
		if got := xerr.KindFrom(err); got != xerr.KindUnavailable {
			t.Fatalf("EnsureConn during Close = %v, want unavailable", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("lazy connection setup did not finish")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish after connection setup")
	}
	if tr.Connected() {
		t.Fatal("transport retained a connection after Close")
	}
}

func TestEnsureConnReturnsCanceledContext(t *testing.T) {
	tr := New("nats://127.0.0.1:4222")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := tr.EnsureConn(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("EnsureConn with canceled context = %v, want context.Canceled", err)
	}
	if tr.Connected() {
		t.Fatal("EnsureConn dialed with a canceled context")
	}
}

func TestEnsureConnCancelsInitialRetry(t *testing.T) {
	url := unreachableNATSURL(t)
	tr := New(url)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	_, err := tr.EnsureConn(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("EnsureConn while NATS is unavailable = %v, want context deadline", err)
	}
	if tr.Connected() {
		t.Fatal("transport reports connected after the initial retry was canceled")
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close after canceled dial: %v", err)
	}
}

func TestCloseCancelsInitialRetry(t *testing.T) {
	optionEntered := make(chan struct{})
	tr := New(unreachableNATSURL(t), WithNATSOptions(func(*nats.Options) error {
		close(optionEntered)
		return nil
	}))
	connectDone := make(chan error, 1)
	go func() {
		_, err := tr.EnsureConn(context.Background())
		connectDone <- err
	}()
	select {
	case <-optionEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("initial NATS connection setup did not start")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- tr.Close() }()
	select {
	case err := <-connectDone:
		if got := xerr.KindFrom(err); got != xerr.KindUnavailable {
			t.Fatalf("EnsureConn during Close = %v, want unavailable", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not cancel the initial retry")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish after canceling the retry")
	}
}

func unreachableNATSURL(t *testing.T) string {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve unreachable NATS port: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release unreachable NATS port: %v", err)
	}
	return "nats://" + address
}
