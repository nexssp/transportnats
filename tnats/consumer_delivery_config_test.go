package tnats_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/action"

	"github.com/nexssp/transportnats/tnats"
)

func TestConsumerUpgradesExistingDurableForDLQRetries(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "COMPAT_STREAM", Subjects: []string{"compat.events"}}); err != nil {
		t.Fatalf("add stream: %v", err)
	}
	legacySub, err := js.PullSubscribe("compat.events", "compat-worker",
		nats.BindStream("COMPAT_STREAM"),
		nats.AckExplicit(),
		nats.AckWait(30*time.Second),
		nats.MaxDeliver(3),
		nats.MaxAckPending(256),
		nats.DeliverAll(),
		nats.ReplayInstant(),
	)
	if err != nil {
		t.Fatalf("create existing durable: %v", err)
	}
	if err := legacySub.Unsubscribe(); err != nil {
		t.Fatalf("unsubscribe existing durable: %v", err)
	}

	binding := tnats.Consumer("COMPAT_STREAM", "compat.events", "compat-worker").ToDeadLetter("compat.dlq")
	binding.MaxDeliver = 3
	worker := action.New("compat.worker", func(_ context.Context, input any) (any, error) {
		return input, nil
	}).Route(binding).Build()
	tr := tnats.New(natsURL)
	tr.Mount([]action.AnyAction{worker})
	ctx := t.Context()
	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	info, err := js.ConsumerInfo("COMPAT_STREAM", "compat-worker")
	if err != nil {
		t.Fatalf("consumer info: %v", err)
	}
	if info.Config.MaxDeliver != -1 {
		t.Fatalf("broker MaxDeliver = %d, want unlimited (-1) for retryable DLQ", info.Config.MaxDeliver)
	}
}
