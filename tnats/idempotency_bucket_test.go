package tnats

import (
	"strings"
	"testing"

	"github.com/nexssp/kernel/action"
)

func TestWithKVIdempotencyBucketTrimsName(t *testing.T) {
	transport := New("nats://127.0.0.1:4222", WithKVIdempotencyBucket(" AGENT_IDEMPOTENCY "))
	if transport.idempotencyBucket != "AGENT_IDEMPOTENCY" {
		t.Fatalf("idempotency bucket = %q, want trimmed bucket name", transport.idempotencyBucket)
	}
}

func TestConfigureIdempotencyStoreRequiresOneStoreAndJetStream(t *testing.T) {
	bucketOnly := New("nats://127.0.0.1:4222", WithKVIdempotencyBucket("AGENT_IDEMPOTENCY"))
	if err := bucketOnly.configureIdempotencyStore(nil); err == nil || !strings.Contains(err.Error(), "JetStream") {
		t.Fatalf("configureIdempotencyStore(nil) error = %v, want JetStream requirement", err)
	}

	both := New(
		"nats://127.0.0.1:4222",
		WithIdempotencyStore(action.NewMemoryIdempotencyStore(0)),
		WithKVIdempotencyBucket("AGENT_IDEMPOTENCY"),
	)
	if err := both.configureIdempotencyStore(nil); err == nil || !strings.Contains(err.Error(), "not both") {
		t.Fatalf("configureIdempotencyStore() error = %v, want mutually exclusive store configuration", err)
	}
}
