package tnats

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestValidateDurableBindingRequiresCompleteResultConfigAndIdentity(t *testing.T) {
	base := DurableWork("JOBS", "jobs.work", "workers", "jobs.dlq")
	cases := []struct {
		name    string
		binding DurableBinding
		wantErr bool
	}{
		{name: "unset preserves legacy", binding: base},
		{name: "missing subject", binding: base.WithDeliveryIdentityField("job_id").WithResult("RESULTS", ""), wantErr: true},
		{name: "missing stream", binding: base.WithDeliveryIdentityField("job_id").WithResult("", "jobs.results"), wantErr: true},
		{name: "missing identity", binding: base.WithResult("RESULTS", "jobs.results"), wantErr: true},
		{name: "wildcard star", binding: base.WithDeliveryIdentityField("job_id").WithResult("RESULTS", "jobs.*"), wantErr: true},
		{name: "wildcard tail", binding: base.WithDeliveryIdentityField("job_id").WithResult("RESULTS", "jobs.>"), wantErr: true},
		{name: "valid result config", binding: base.WithDeliveryIdentityField("job_id").WithResult("RESULTS", "jobs.results")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateDurableBinding(tc.binding)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validateDurableBinding() error = %v, wantErr %t", err, tc.wantErr)
			}
		})
	}
}

func TestDurableResultIdentityIsStableAcrossAliases(t *testing.T) {
	first := durableResultMessageID("job-42")
	if got := durableResultMessageID("job-42"); got != first {
		t.Fatalf("same job ID alias = %q, want stable %q", got, first)
	}
	if got := durableResultMessageID("job-43"); got == first {
		t.Fatalf("different job ID produced same result ID %q", got)
	}
	if got := durableResultStorageKey("unsafe/job.id with spaces"); got == "unsafe/job.id with spaces" || got == "" {
		t.Fatalf("result storage key = %q, want a safe stable key", got)
	}
}

func TestValidateDurableResultConfigRequiresPersistentNonExpiringRetention(t *testing.T) {
	base := nats.StreamConfig{
		Name:      "RESULTS",
		Subjects:  []string{"jobs.>"},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Discard:   nats.DiscardNew,
	}
	cases := []struct {
		name   string
		config nats.StreamConfig
		wantOK bool
	}{
		{name: "durable result stream", config: base, wantOK: true},
		{name: "memory storage", config: withResultConfig(base, func(c *nats.StreamConfig) { c.Storage = nats.MemoryStorage })},
		{name: "interest retention", config: withResultConfig(base, func(c *nats.StreamConfig) { c.Retention = nats.InterestPolicy })},
		{name: "discard old", config: withResultConfig(base, func(c *nats.StreamConfig) { c.Discard = nats.DiscardOld })},
		{name: "expiry", config: withResultConfig(base, func(c *nats.StreamConfig) { c.MaxAge = time.Hour })},
		{name: "wrong subject", config: withResultConfig(base, func(c *nats.StreamConfig) { c.Subjects = []string{"elsewhere.>"} })},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateDurableResultConfig(tc.config, "jobs.completed")
			if (err == nil) != tc.wantOK {
				t.Fatalf("validateDurableResultConfig() error = %v, wantOK %t", err, tc.wantOK)
			}
		})
	}
}

func withResultConfig(base nats.StreamConfig, update func(*nats.StreamConfig)) nats.StreamConfig {
	config := base
	update(&config)
	return config
}
