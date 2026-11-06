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
	ctx := t.Context()

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if _, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "STRICT_BUCKET"}); err != nil {
		t.Fatalf("create object store: %v", err)
	}
	var failureCount atomic.Int32
	receivedObject := make(chan struct{}, 1)
	objAct := action.New("obj.strict", func(_ context.Context, ev tnats.ObjectEvent) (string, error) {
		if len(ev.Data) == 0 {
			failureCount.Add(1)
		}
		select {
		case receivedObject <- struct{}{}:
		default:
		}
		return "ok", nil
	}).Route(tnats.ObjectStore("STRICT_BUCKET", ">").WithData(50 * 1024)).Build()
	tr.Mount([]action.AnyAction{objAct})

	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	payload := bytes.Repeat([]byte("A"), 200*1024)
	if _, err := tr.PutObject(ctx, "STRICT_BUCKET", "large.bin", payload); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	select {
	case <-receivedObject:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ObjectStore event")
	}
	if failureCount.Load() != 1 {
		t.Fatalf("oversized object event should omit data, got %d failures", failureCount.Load())
	}

	_, getErr := tr.GetObject(ctx, "STRICT_BUCKET", "large.bin")
	if getErr != nil {
		t.Fatalf("GetObject error: %v", getErr)
	}
}

func TestConsumer_MultiFilter_HappyPath(t *testing.T) {
	t.Parallel()
	_, natsURL := runEmbeddedNATS(t)

	var receivedCount atomic.Int32
	received := make(chan struct{}, 2)
	multiBinding := tnats.Consumer("MULTI_STREAM", "events.orders.eu", "multi-filter-worker").
		WithFilters("events.orders.eu", "events.orders.us")

	multiAct := action.New("orders.multifilter", func(_ context.Context, _ OrderReq) (string, error) {
		receivedCount.Add(1)
		received <- struct{}{}

		return "ok", nil
	}).Route(multiBinding).Build()

	tr := tnats.New(natsURL)
	tr.Mount([]action.AnyAction{multiAct})

	ctx := t.Context()

	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	nc := tr.Conn()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	d1, err := json.Marshal(OrderReq{ID: "EU-1", Price: 100})
	if err != nil {
		t.Fatalf("marshal EU request: %v", err)
	}
	d2, err := json.Marshal(OrderReq{ID: "US-1", Price: 200})
	if err != nil {
		t.Fatalf("marshal US request: %v", err)
	}

	if _, err := js.Publish("events.orders.eu", d1); err != nil {
		t.Fatalf("publish EU: %v", err)
	}
	if _, err := js.Publish("events.orders.us", d2); err != nil {
		t.Fatalf("publish US: %v", err)
	}
	for range 2 {
		select {
		case <-received:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for matching consumer event")
		}
	}

	if count := receivedCount.Load(); count != 2 {
		t.Fatalf("expected exactly 2 matching multi-filter events, got %d", count)
	}
	info, err := js.ConsumerInfo("MULTI_STREAM", "multi-filter-worker")
	if err != nil {
		t.Fatalf("consumer info: %v", err)
	}
	if len(info.Config.FilterSubjects) != 2 || info.Config.FilterSubjects[0] != "events.orders.eu" || info.Config.FilterSubjects[1] != "events.orders.us" {
		t.Fatalf("unexpected consumer filter subjects: %v", info.Config.FilterSubjects)
	}
}

func TestConsumer_PoisonPillWithoutDLQ_Terminates(t *testing.T) {
	t.Parallel()
	_, natsURL := runEmbeddedNATS(t)

	var deliveries atomic.Int32
	seenDelivery := make(chan struct{}, 3)
	binding := tnats.Consumer("POISON_STREAM", "poison.subject", "poison-worker").
		NewOnly().
		WithBackOff(10*time.Millisecond, 20*time.Millisecond)
	binding.MaxDeliver = 3

	failAct := action.New("poison.action", func(_ context.Context, _ OrderReq) (string, error) {
		deliveries.Add(1)
		select {
		case seenDelivery <- struct{}{}:
		default:
		}

		return "", errors.New("unrecoverable database corruption")
	}).Route(binding).Build()

	tr := tnats.New(natsURL)
	tr.Mount([]action.AnyAction{failAct})

	ctx := t.Context()

	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	nc := tr.Conn()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	payload, err := json.Marshal(OrderReq{ID: "POISON-1", Price: 1})
	if err != nil {
		t.Fatalf("marshal poison request: %v", err)
	}
	if _, err := js.Publish("poison.subject", payload); err != nil {
		t.Fatalf("publish poison request: %v", err)
	}
	for range 3 {
		select {
		case <-seenDelivery:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for poison-message delivery")
		}
	}

	if got := deliveries.Load(); got != 3 {
		t.Fatalf("poison pill was delivered %d times; expected 3 attempts", got)
	}
}
