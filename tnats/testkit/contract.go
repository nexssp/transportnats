package testkit

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/transportnats/tnats"
)

type EmbeddedServer struct {
	Server *natsserver.Server
	URL    string
}

func StartEmbedded(t *testing.T, jetStream bool) *EmbeddedServer {
	t.Helper()

	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("testkit: reserve port: %v", err)
	}

	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()

	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      port,
		JetStream: jetStream,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}

	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("testkit: new server: %v", err)
	}

	go srv.Start()

	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("testkit: server startup timed out")
	}

	t.Cleanup(func() {
		srv.Shutdown()
		srv.WaitForShutdown()
	})

	return &EmbeddedServer{
		Server: srv,
		URL:    fmt.Sprintf("nats://127.0.0.1:%d", port),
	}
}

func NewTransport(t *testing.T, srv *EmbeddedServer, opts ...tnats.Option) *tnats.Transport {
	t.Helper()

	return tnats.New(srv.URL, opts...)
}

func RunContract(t *testing.T, newTransport func(t *testing.T, url string) *tnats.Transport) {
	t.Helper()

	srv := StartEmbedded(t, false)
	tr := newTransport(t, srv.URL)

	done := make(chan struct{}, 1)
	act := action.New("contract.ping", func(ctx context.Context, in map[string]string) (string, error) {
		select {
		case done <- struct{}{}:
		default:
		}

		return "pong", nil
	}).Route(tnats.Topic("contract.ping")).Build()

	tr.Mount([]action.AnyAction{act})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _, _ = tr.Do(ctx, nil) }()

	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	if err := tr.Publish(ctx, "contract.ping", map[string]string{"hi": "there"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("contract: no message received within deadline")
	}
}
