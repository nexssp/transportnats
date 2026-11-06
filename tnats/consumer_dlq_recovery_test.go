package tnats_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/action"

	"github.com/nexssp/transportnats/tnats"
)

func TestConsumerRetriesDeadLetterPublishAfterStreamRecovery(t *testing.T) {
	t.Parallel()
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
	if _, err := js.AddStream(&nats.StreamConfig{Name: "RECOVERY_STREAM", Subjects: []string{"recovery.events"}}); err != nil {
		t.Fatalf("add source stream: %v", err)
	}

	binding := tnats.Consumer("RECOVERY_STREAM", "recovery.events", "recovery-worker").ToDeadLetter("recovery.dlq")
	binding.MaxDeliver = 1
	binding = binding.WithBackOff(25 * time.Millisecond)
	var actionCalls atomic.Int32
	worker := action.New("recovery.worker", func(_ context.Context, _ any) (any, error) {
		actionCalls.Add(1)
		return nil, errors.New("poison message")
	}).Route(binding).Build()

	tr := tnats.New(natsURL)
	tr.Mount([]action.AnyAction{worker})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	done := make(chan error, 1)
	go func() {
		_, runErr := tr.Do(ctx, nil)
		done <- runErr
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("transport did not stop")
		}
		if err := tr.Close(); err != nil {
			t.Errorf("close transport: %v", err)
		}
	}()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	nakSub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.CONSUMER.MSG_NAKED.RECOVERY_STREAM.recovery-worker")
	if err != nil {
		t.Fatalf("subscribe to NAK advisories: %v", err)
	}
	defer func() {
		if err := nakSub.Unsubscribe(); err != nil {
			t.Errorf("unsubscribe NAK advisories: %v", err)
		}
	}()
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush NAK advisory subscription: %v", err)
	}
	if err := js.DeleteStream("RECOVERY_STREAM_DLQ"); err != nil {
		t.Fatalf("remove DLQ stream: %v", err)
	}
	if _, err := js.Publish("recovery.events", []byte(`"bad"`)); err != nil {
		t.Fatalf("publish poison message: %v", err)
	}
	if _, err := nakSub.NextMsg(5 * time.Second); err != nil {
		t.Fatalf("wait for NAK after failed DLQ publish: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "RECOVERY_STREAM_DLQ", Subjects: []string{"recovery.dlq"}}); err != nil {
		t.Fatalf("restore DLQ stream: %v", err)
	}

	dlqSub, err := js.PullSubscribe("recovery.dlq", "recovery-dlq-observer", nats.BindStream("RECOVERY_STREAM_DLQ"))
	if err != nil {
		t.Fatalf("subscribe to recovered DLQ: %v", err)
	}
	defer func() {
		if err := dlqSub.Unsubscribe(); err != nil {
			t.Errorf("unsubscribe DLQ observer: %v", err)
		}
	}()
	messages, err := dlqSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("fetch recovered DLQ message: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("DLQ message count = %d, want 1", len(messages))
	}
	if got := actionCalls.Load(); got != 1 {
		t.Fatalf("poison action calls = %d, want exactly 1", got)
	}
}

func TestDurableRetriesDeadLetterPublishAfterStreamRecovery(t *testing.T) {
	t.Parallel()
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

	binding := tnats.DurableWork(
		"DURABLE_RECOVERY", "durable.recovery.events", "durable-recovery-worker", "durable.recovery.dlq",
	)
	binding.MaxDeliver = 1
	binding.RetryDelay = 25 * time.Millisecond
	var actionCalls atomic.Int32
	worker := action.New("durable.recovery.worker", func(_ context.Context, _ any) (any, error) {
		actionCalls.Add(1)
		return nil, errors.New("poison message")
	}).Route(binding).Build()

	tr := tnats.New(natsURL)
	tr.Mount([]action.AnyAction{worker})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	done := make(chan error, 1)
	go func() {
		_, runErr := tr.Do(ctx, nil)
		done <- runErr
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("transport did not stop")
		}
		if err := tr.Close(); err != nil {
			t.Errorf("close transport: %v", err)
		}
	}()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	nakSub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.CONSUMER.MSG_NAKED.DURABLE_RECOVERY.durable-recovery-worker")
	if err != nil {
		t.Fatalf("subscribe to NAK advisories: %v", err)
	}
	defer func() {
		if err := nakSub.Unsubscribe(); err != nil {
			t.Errorf("unsubscribe NAK advisories: %v", err)
		}
	}()
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush NAK advisory subscription: %v", err)
	}
	if err := js.DeleteStream("DURABLE_RECOVERY_DLQ"); err != nil {
		t.Fatalf("remove DLQ stream: %v", err)
	}
	if _, err := js.Publish("durable.recovery.events", []byte(`"bad"`)); err != nil {
		t.Fatalf("publish poison message: %v", err)
	}
	if _, err := nakSub.NextMsg(5 * time.Second); err != nil {
		t.Fatalf("wait for NAK after failed DLQ publish: %v", err)
	}
	if _, err := js.AddStream(&nats.StreamConfig{Name: "DURABLE_RECOVERY_DLQ", Subjects: []string{"durable.recovery.dlq"}}); err != nil {
		t.Fatalf("restore DLQ stream: %v", err)
	}

	dlqSub, err := js.PullSubscribe(
		"durable.recovery.dlq", "durable-recovery-dlq-observer", nats.BindStream("DURABLE_RECOVERY_DLQ"),
	)
	if err != nil {
		t.Fatalf("subscribe to recovered DLQ: %v", err)
	}
	defer func() {
		if err := dlqSub.Unsubscribe(); err != nil {
			t.Errorf("unsubscribe DLQ observer: %v", err)
		}
	}()
	messages, err := dlqSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("fetch recovered DLQ message: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("DLQ message count = %d, want 1", len(messages))
	}
	if got := actionCalls.Load(); got != 1 {
		t.Fatalf("poison action calls = %d, want exactly 1", got)
	}
}
