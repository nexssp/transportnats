package tnats_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/transport"
	"github.com/nexssp/transportnats/tnats"
)

func TestTransportImplementsInterface(t *testing.T) {
	var _ transport.Transport = (*tnats.Transport)(nil)
}

func runEmbeddedNATS(t *testing.T) (*natsserver.Server, string) {
	t.Helper()

	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed finding free test port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()

	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      port,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}

	server, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("failed starting embedded NATS server: %v", err)
	}

	go server.Start()
	if !server.ReadyForConnections(5 * time.Second) {
		t.Fatal("embedded NATS server startup timed out")
	}

	t.Cleanup(func() {
		server.Shutdown()
		server.WaitForShutdown()
	})

	return server, fmt.Sprintf("nats://127.0.0.1:%d", port)
}

type OrderReq struct {
	ID    string `json:"id"`
	Price int    `json:"price"`
}

type OrderRes struct {
	Confirmation string `json:"confirmation"`
}

func TestTNATS_PubSubAndQueueGroup(t *testing.T) {
	t.Parallel()
	_, natsURL := runEmbeddedNATS(t)

	var count atomic.Int32
	received := make(chan struct{}, 5)
	orderAct := action.New("order.process", func(ctx context.Context, req OrderReq) (string, error) {
		count.Add(1)
		received <- struct{}{}

		return "ok", nil
	}).Route(tnats.Topic("orders.created", "workers")).Build()

	tr := tnats.New(natsURL)
	tr.Mount([]action.AnyAction{orderAct})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady failed: %v", err)
	}

	for i := 1; i <= 5; i++ {
		err := tr.Publish(ctx, "orders.created", OrderReq{ID: fmt.Sprintf("ord_%d", i), Price: i * 10})
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}

	for i := 0; i < 5; i++ {
		select {
		case <-received:
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for message %d", i+1)
		}
	}

	if got := count.Load(); got != 5 {
		t.Fatalf("expected 5 executions, got %d", got)
	}
}

func TestTNATS_RequestReply_RPC_SuccessAndError(t *testing.T) {
	t.Parallel()
	_, natsURL := runEmbeddedNATS(t)

	rpcAct := action.New("order.rpc", func(ctx context.Context, req OrderReq) (OrderRes, error) {
		if req.Price <= 0 {
			return OrderRes{}, xerr.BadRequest("price must be positive")
		}

		return OrderRes{Confirmation: "CONFIRMED:" + req.ID}, nil
	}).Route(tnats.Request("order.query", 2*time.Second)).Build()

	tr := tnats.New(natsURL)
	tr.Mount([]action.AnyAction{rpcAct})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady failed: %v", err)
	}

	var res OrderRes
	err := tr.Request(ctx, "order.query", OrderReq{ID: "ord_999", Price: 150}, &res)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	if res.Confirmation != "CONFIRMED:ord_999" {
		t.Fatalf("unexpected RPC response: %+v", res)
	}

	var failRes OrderRes
	err = tr.Request(ctx, "order.query", OrderReq{ID: "ord_invalid", Price: -10}, &failRes)
	if err == nil {
		t.Fatal("expected request error, got nil")
	}

	var appErr *xerr.AppError
	if !errors.As(err, &appErr) {
		t.Fatalf("expected *xerr.AppError, got: %T (%v)", err, err)
	}
}

func TestTNATS_Consumer_DeliveryAndDeadLetter(t *testing.T) {
	t.Parallel()
	_, natsURL := runEmbeddedNATS(t)

	dlqCh := make(chan tnats.DeadLetter, 1)
	binding := tnats.Consumer("TEST_EVENTS", "events.orders.*", "test-worker").
		WithBackOff(10*time.Millisecond, 20*time.Millisecond).
		ToDeadLetter("events.dlq")
	binding.MaxDeliver = 3

	failingAct := action.New("consumer.fail", func(ctx context.Context, req OrderReq) (string, error) {
		return "", errors.New("business failure")
	}).Route(binding).Build()

	tr := tnats.New(natsURL)
	tr.Mount([]action.AnyAction{failingAct})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	nc := tr.Conn()
	_, err := nc.Subscribe("events.dlq", func(msg *nats.Msg) {
		var dl tnats.DeadLetter
		_ = json.Unmarshal(msg.Data, &dl)
		dlqCh <- dl
	})
	if err != nil {
		t.Fatalf("subscribe DLQ: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	data, _ := json.Marshal(OrderReq{ID: "ord_fail_1", Price: 100})
	if _, err := js.Publish("events.orders.created", data); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case dl := <-dlqCh:
		if dl.Subject != "events.orders.created" {
			t.Fatalf("expected subject events.orders.created, got %s", dl.Subject)
		}
		if dl.Deliveries < 2 {
			t.Fatalf("expected deliveries >= 2, got %d", dl.Deliveries)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("DLQ message not received within deadline")
	}
}

func TestTNATS_ObjectStore_LifecycleAndWatch(t *testing.T) {
	t.Parallel()
	_, natsURL := runEmbeddedNATS(t)

	received := make(chan tnats.ObjectEvent, 2)
	objAct := action.New("obj.handler", func(ctx context.Context, ev tnats.ObjectEvent) (string, error) {
		received <- ev

		return "ok", nil
	}).Route(tnats.ObjectStore("TEST_BUCKET", "reports.>").WithData(1024 * 1024)).Build()

	tr := tnats.New(natsURL)
	tr.Mount([]action.AnyAction{objAct})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	if _, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "TEST_BUCKET"}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	if _, err := tr.PutObject(ctx, "TEST_BUCKET", "reports.finance.q4", []byte("Q4_REPORT_CONTENT")); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	select {
	case ev := <-received:
		if ev.Name != "reports.finance.q4" || ev.Type != tnats.ObjectPut {
			t.Fatalf("unexpected event: %+v", ev)
		}
		if string(ev.Data) != "Q4_REPORT_CONTENT" {
			t.Fatalf("unexpected content: %s", string(ev.Data))
		}
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for ObjectStore PUT event")
	}

	data, err := tr.GetObject(ctx, "TEST_BUCKET", "reports.finance.q4")
	if err != nil || string(data) != "Q4_REPORT_CONTENT" {
		t.Fatalf("GetObject failed: %v, got %s", err, string(data))
	}

	if err := tr.DeleteObject(ctx, "TEST_BUCKET", "reports.finance.q4"); err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	select {
	case ev := <-received:
		if ev.Name != "reports.finance.q4" || ev.Type != tnats.ObjectDelete {
			t.Fatalf("unexpected delete event: %+v", ev)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for ObjectStore DELETE event")
	}
}

func TestTNATS_Micro_Service_RPC(t *testing.T) {
	t.Parallel()
	_, natsURL := runEmbeddedNATS(t)

	microAct := action.New("pricing.get", func(ctx context.Context, in OrderReq) (OrderRes, error) {
		return OrderRes{Confirmation: "PRICE_APPROVED:" + in.ID}, nil
	}).Route(tnats.Service("pricing-svc", "1.0.0", "calculate", "pricing.v1.calc")).Build()

	tr := tnats.New(natsURL)
	tr.Mount([]action.AnyAction{microAct})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	var res OrderRes
	if err := tr.Request(ctx, "pricing.v1.calc", OrderReq{ID: "ORDER_ABC", Price: 500}, &res); err != nil {
		t.Fatalf("micro request failed: %v", err)
	}

	if res.Confirmation != "PRICE_APPROVED:ORDER_ABC" {
		t.Fatalf("unexpected response: %+v", res)
	}
}

func TestTNATS_EmbeddedServer_Integration(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	tr := tnats.New("", tnats.WithEmbeddedServer(tnats.EmbeddedConfig{
		Port:      0,
		JetStream: true,
		StoreDir:  tmpDir,
		NoLog:     true,
		NoSigs:    true,
	}))

	readyAct := action.New("embedded.ping", func(ctx context.Context, _ any) (string, error) {
		return "PONG", nil
	}).Route(tnats.Request("embedded.ping")).Build()
	tr.Mount([]action.AnyAction{readyAct})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := tr.Do(ctx, nil)
		errCh <- err
	}()

	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady on embedded server: %v", err)
	}

	var res string
	if err := tr.Request(ctx, "embedded.ping", map[string]string{}, &res); err != nil {
		t.Fatalf("embedded RPC failed: %v", err)
	}
	if res != "PONG" {
		t.Fatalf("expected PONG, got %s", res)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Do exited with error: %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("embedded server shutdown timed out")
	}
}

func TestTNATS_Idempotency_PanicRecovery(t *testing.T) {
	t.Parallel()
	_, natsURL := runEmbeddedNATS(t)

	var runs atomic.Int32
	panickingAct := action.New("order.panic.idem", func(ctx context.Context, req OrderReq) (OrderRes, error) {
		n := runs.Add(1)
		if n == 1 {
			panic("unexpected runtime panic")
		}

		return OrderRes{Confirmation: "RECOVERED:" + req.ID}, nil
	}).
		Idempotent().
		Route(tnats.Request("order.panic.rpc", 2*time.Second)).
		Build()

	tr := tnats.New(natsURL)
	tr.Mount([]action.AnyAction{panickingAct})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	var res OrderRes
	_ = tr.Request(ctx, "order.panic.rpc", OrderReq{ID: "IDEM_PANIC_1", Price: 100}, &res)

	err := tr.Request(ctx, "order.panic.rpc", OrderReq{ID: "IDEM_PANIC_1", Price: 100}, &res)
	if err != nil {
		t.Fatalf("second call failed after panic recovery: %v", err)
	}
	if res.Confirmation != "RECOVERED:IDEM_PANIC_1" {
		t.Fatalf("expected RECOVERED:IDEM_PANIC_1, got %s", res.Confirmation)
	}
}

func TestTNATS_Auth_InvalidNKey_GracefulFailure(t *testing.T) {
	t.Parallel()

	tr := tnats.New("nats://127.0.0.1:4222", tnats.WithNKeySeed("/path/that/does/not/exist.seed"))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() { _, _ = tr.Do(ctx, nil) }()

	err := tr.WaitReady(ctx)
	if err == nil {
		t.Fatal("expected error for invalid nkey seed, got nil")
	}
}

func TestTNATS_PublishDurable_CachedInfra(t *testing.T) {
	t.Parallel()
	_, natsURL := runEmbeddedNATS(t)

	binding := tnats.DurableWork("CACHED_STREAM", "cached.orders", "cached-worker", "cached.dlq")
	tr := tnats.New(natsURL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	for i := 0; i < 50; i++ {
		if err := tr.PublishDurable(ctx, binding, OrderReq{ID: fmt.Sprintf("ord_%d", i), Price: i}); err != nil {
			t.Fatalf("PublishDurable failed on iteration %d: %v", i, err)
		}
	}
}
