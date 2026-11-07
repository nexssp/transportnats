package tnats

import (
	"context"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/transport/codec"
	"github.com/nexssp/transport/identity"
)

func TestApplyDurableDeliveryIdentityKeepsRequestIDAndRefreshesExecutionID(t *testing.T) {
	parent := xctx.WithExecutionID(context.Background(), "listener-execution")
	parent = xctx.WithRootExecutionID(parent, "listener-root")
	binding := DurableWork("AGENT_JOBS", "agent.jobs", "workers", "agent.jobs.dlq").WithDeliveryIdentityField("job_id")
	message := &nats.Msg{
		Subject: "agent.jobs",
		Data:    []byte(`{"job_id":"job-42"}`),
		Header: nats.Header{
			identity.HeaderExecutionID: []string{"producer-execution"},
		},
	}

	var executionIDs []string
	for range 2 {
		ctx, _, release := xctx.NewScope(parent)
		ctx, err := applyDurableDeliveryIdentity(ctx, message, binding, codec.Default)
		if err != nil {
			release()
			t.Fatalf("apply delivery identity: %v", err)
		}
		requestID := xctx.RequestIDFrom(ctx)
		executionID := xctx.ExecutionIDFrom(ctx)
		rootExecutionID := xctx.RootExecutionIDFrom(ctx)
		if requestID != "job-42" {
			release()
			t.Fatalf("request ID = %q, want stable job-42", requestID)
		}
		if executionID == "" || executionID == "listener-execution" || executionID == "producer-execution" {
			release()
			t.Fatalf("execution ID = %q, want a fresh delivery ID", executionID)
		}
		if rootExecutionID != executionID {
			release()
			t.Fatalf("root execution ID = %q, want delivery ID %q", rootExecutionID, executionID)
		}
		if message.Header.Get(identity.HeaderRequestID) != requestID || message.Header.Get(idempotencyKeyHeader) != requestID {
			release()
			t.Fatalf("identity headers = %#v, want stable request and idempotency IDs", message.Header)
		}
		if message.Header.Get(identity.HeaderExecutionID) != executionID {
			release()
			t.Fatalf("execution header = %q, want %q", message.Header.Get(identity.HeaderExecutionID), executionID)
		}
		executionIDs = append(executionIDs, executionID)
		release()
	}
	if executionIDs[0] == executionIDs[1] {
		t.Fatalf("redelivery execution IDs = %#v, want distinct IDs", executionIDs)
	}
}

func TestApplyDurableDeliveryIdentityRejectsInvalidOrConflictingIdentity(t *testing.T) {
	binding := DurableWork("AGENT_JOBS", "agent.jobs", "workers", "agent.jobs.dlq").WithDeliveryIdentityField("job_id")
	tests := []struct {
		name    string
		message *nats.Msg
	}{
		{
			name:    "missing field",
			message: &nats.Msg{Data: []byte(`{"route":"memory"}`)},
		},
		{
			name:    "non-string field",
			message: &nats.Msg{Data: []byte(`{"job_id":42}`)},
		},
		{
			name:    "oversized field",
			message: &nats.Msg{Data: []byte(`{"job_id":"` + strings.Repeat("x", 257) + `"}`)},
		},
		{
			name: "conflicting idempotency key",
			message: &nats.Msg{
				Data:   []byte(`{"job_id":"job-42"}`),
				Header: nats.Header{idempotencyKeyHeader: []string{"other-job"}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, _, release := xctx.NewScope(context.Background())
			defer release()
			if _, err := applyDurableDeliveryIdentity(ctx, test.message, binding, codec.Default); err == nil {
				t.Fatal("applyDurableDeliveryIdentity() succeeded, want validation error")
			}
		})
	}
}

func TestApplyDurableDeliveryIdentityIsOptIn(t *testing.T) {
	ctx, _, release := xctx.NewScope(context.Background())
	defer release()
	message := &nats.Msg{Data: []byte(`{"job_id":"job-42"}`)}
	got, err := applyDurableDeliveryIdentity(ctx, message, DurableWork("AGENT_JOBS", "agent.jobs", "workers", "agent.jobs.dlq"), codec.Default)
	if err != nil {
		t.Fatalf("apply default delivery identity: %v", err)
	}
	if got != ctx || xctx.RequestIDFrom(got) != "" || xctx.ExecutionIDFrom(got) != "" || message.Header != nil {
		t.Fatalf("default binding changed identity or headers: request=%q execution=%q headers=%#v", xctx.RequestIDFrom(got), xctx.ExecutionIDFrom(got), message.Header)
	}
}
