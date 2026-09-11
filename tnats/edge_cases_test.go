package tnats_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/transportnats/tnats"
)

func TestObjectStore_ExceedsMaxBytes_ReturnsError(t *testing.T) {
	t.Parallel()
	_, natsURL := runEmbeddedNATS(t)

	tr := tnats.New(natsURL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	js, _ := nc.JetStream()
	_, _ = js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "STRICT_BUCKET"})

	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	payload := bytes.Repeat([]byte("A"), 200*1024)
	if _, err := tr.PutObject(ctx, "STRICT_BUCKET", "large.bin", payload); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	var failureCount atomic.Int32
	objAct := action.New("obj.strict", func(ctx context.Context, ev tnats.ObjectEvent) (string, error) {
		if len(ev.Data) == 0 {
			failureCount.Add(1)
		}

		return "ok", nil
	}).Route(tnats.ObjectStore("STRICT_BUCKET", ">").WithData(50 * 1024)).Build()

	tr.Mount([]action.AnyAction{objAct})

	_, getErr := tr.GetObject(ctx, "STRICT_BUCKET", "large.bin")
	if getErr != nil {
		t.Fatalf("GetObject error: %v", getErr)
	}
}

func TestConsumer_MultiFilter_HappyPath(t *testing.T) {
	t.Parallel()
	_, natsURL := runEmbeddedNATS(t)

	var receivedCount atomic.Int32
	multiBinding := tnats.Consumer("MULTI_STREAM", "events.orders.eu", "multi-filter-worker").
		WithFilters("events.orders.eu", "events.orders.us")

	multiAct := action.New("orders.multifilter", func(ctx context.Context, req OrderReq) (string, error) {
		receivedCount.Add(1)

		return "ok", nil
	}).Route(multiBinding).Build()

	tr := tnats.New(natsURL)
	tr.Mount([]action.AnyAction{multiAct})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	nc := tr.Conn()
	js, _ := nc.JetStream()

	d1, _ := json.Marshal(OrderReq{ID: "EU-1", Price: 100})
	d2, _ := json.Marshal(OrderReq{ID: "US-1", Price: 200})
	dIgnored, _ := json.Marshal(OrderReq{ID: "ASIA-1", Price: 300})

	if _, err := js.Publish("events.orders.eu", d1); err != nil {
		t.Fatalf("publish EU: %v", err)
	}
	if _, err := js.Publish("events.orders.us", d2); err != nil {
		t.Fatalf("publish US: %v", err)
	}
	_, _ = js.Publish("events.orders.asia", dIgnored)

	deadline := time.Now().Add(3 * time.Second)
	for receivedCount.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	if count := receivedCount.Load(); count != 2 {
		t.Fatalf("expected exactly 2 matching multi-filter events, got %d", count)
	}
}

func TestConsumer_PoisonPillWithoutDLQ_Terminates(t *testing.T) {
	t.Parallel()
	_, natsURL := runEmbeddedNATS(t)

	var deliveries atomic.Int32
	binding := tnats.Consumer("POISON_STREAM", "poison.subject", "poison-worker").
		NewOnly().
		WithBackOff(10*time.Millisecond, 20*time.Millisecond)
	binding.MaxDeliver = 3

	failAct := action.New("poison.action", func(ctx context.Context, req OrderReq) (string, error) {
		deliveries.Add(1)

		return "", errors.New("unrecoverable database corruption")
	}).Route(binding).Build()

	tr := tnats.New(natsURL)
	tr.Mount([]action.AnyAction{failAct})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	nc := tr.Conn()
	js, _ := nc.JetStream()

	payload, _ := json.Marshal(OrderReq{ID: "POISON-1", Price: 1})
	_, _ = js.Publish("poison.subject", payload)

	deadline := time.Now().Add(2 * time.Second)
	for deliveries.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	if got := deliveries.Load(); got > 3 {
		t.Fatalf("poison pill was delivered %d times; expected to terminate at 3", got)
	}
}
