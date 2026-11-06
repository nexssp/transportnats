package tnats

import (
	"fmt"
	"testing"
	"time"
)

type retryAfterTestError struct{ delay time.Duration }

func (e retryAfterTestError) Error() string             { return "retry later" }
func (e retryAfterTestError) RetryAfter() time.Duration { return e.delay }

func TestConsumerRetryDelay(t *testing.T) {
	t.Parallel()

	backoff := []time.Duration{100 * time.Millisecond, 400 * time.Millisecond, time.Second}
	binding := Consumer("EVENTS", "events.created", "worker").WithBackOff(backoff...)

	tests := []struct {
		name       string
		deliveries uint64
		err        error
		want       time.Duration
	}{
		{name: "first delivery", deliveries: 1, want: backoff[0]},
		{name: "second delivery", deliveries: 2, want: backoff[1]},
		{name: "zero delivery count", deliveries: 0, want: backoff[0]},
		{name: "schedule capped", deliveries: 99, want: backoff[2]},
		{name: "wrapped RetryAfter overrides schedule", deliveries: 2, err: fmt.Errorf("wrapped: %w", retryAfterTestError{delay: 250 * time.Millisecond}), want: 250 * time.Millisecond},
		{name: "nonpositive RetryAfter uses schedule", deliveries: 1, err: retryAfterTestError{}, want: backoff[0]},
		{name: "default delay", deliveries: 1, want: time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := binding
			if tt.name == "default delay" {
				b.BackOff = nil
			}
			if got := consumerRetryDelay(b, tt.deliveries, tt.err); got != tt.want {
				t.Fatalf("consumerRetryDelay() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestValidateConsumerBindingRejectsInvalidPoliciesAndBackoff(t *testing.T) {
	t.Parallel()

	valid := Consumer("EVENTS", "events.created", "worker")
	tests := []struct {
		name   string
		mutate func(*ConsumerBinding)
	}{
		{name: "delivery policy", mutate: func(b *ConsumerBinding) { b.DeliverPolicy = DeliverPolicy(255) }},
		{name: "ack policy", mutate: func(b *ConsumerBinding) { b.AckPolicy = AckPolicy(255) }},
		{name: "ack none with dead letter", mutate: func(b *ConsumerBinding) {
			b.AckPolicy = AckNone
			b.DeadLetterSubject = "events.dlq"
		}},
		{name: "replay policy", mutate: func(b *ConsumerBinding) { b.ReplayPolicy = ReplayPolicy(255) }},
		{name: "zero start sequence", mutate: func(b *ConsumerBinding) { b.DeliverPolicy = DeliverByStartSequence }},
		{name: "missing start time", mutate: func(b *ConsumerBinding) { b.DeliverPolicy = DeliverByStartTime }},
		{name: "zero backoff", mutate: func(b *ConsumerBinding) { b.BackOff = []time.Duration{0} }},
		{name: "negative backoff", mutate: func(b *ConsumerBinding) { b.BackOff = []time.Duration{-time.Second} }},
		{name: "backoff longer than max deliveries", mutate: func(b *ConsumerBinding) {
			b.MaxDeliver = 1
			b.BackOff = []time.Duration{time.Second, 2 * time.Second}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := valid
			tt.mutate(&b)
			if err := validateConsumerBinding(b); err == nil {
				t.Fatal("validateConsumerBinding() succeeded, want an error")
			}
		})
	}
}

func TestValidateConsumerBindingAcceptsValidConfiguration(t *testing.T) {
	t.Parallel()

	binding := Consumer("EVENTS", "events.created", "worker").
		WithBackOff(100*time.Millisecond, 250*time.Millisecond)
	binding.MaxDeliver = 2
	if err := validateConsumerBinding(binding); err != nil {
		t.Fatalf("validateConsumerBinding() = %v, want nil", err)
	}
}

func TestValidateDurableBindingRejectsMissingFieldsAndInvalidDeliveryPolicy(t *testing.T) {
	t.Parallel()
	valid := DurableWork("EVENTS", "events.created", "worker", "events.dead")
	tests := []struct {
		name   string
		mutate func(*DurableBinding)
	}{
		{name: "stream", mutate: func(b *DurableBinding) { b.Stream = "" }},
		{name: "subject", mutate: func(b *DurableBinding) { b.Subject = "" }},
		{name: "durable", mutate: func(b *DurableBinding) { b.Durable = "" }},
		{name: "dead letter subject", mutate: func(b *DurableBinding) { b.DeadLetterSubject = "" }},
		{name: "max deliveries", mutate: func(b *DurableBinding) { b.MaxDeliver = 0 }},
		{name: "ack wait", mutate: func(b *DurableBinding) { b.AckWait = 0 }},
		{name: "retry delay", mutate: func(b *DurableBinding) { b.RetryDelay = 0 }},
		{name: "max ack pending", mutate: func(b *DurableBinding) { b.MaxAckPending = 0 }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			binding := valid
			test.mutate(&binding)
			if err := validateDurableBinding(binding); err == nil {
				t.Fatal("validateDurableBinding() succeeded, want an error")
			}
		})
	}
}
