package tnats_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nexssp/kernel/xerr"

	"github.com/nexssp/transportnats/tnats"
)

func TestMapErrorClassifications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		kind xerr.Kind
	}{
		{name: "canceled", err: context.Canceled, kind: xerr.KindCanceled},
		{name: "deadline", err: context.DeadlineExceeded, kind: xerr.KindTimeout},
		{name: "NATS timeout", err: nats.ErrTimeout, kind: xerr.KindTimeout},
		{name: "no responders", err: nats.ErrNoResponders, kind: xerr.KindUnavailable},
		{name: "connection closed", err: nats.ErrConnectionClosed, kind: xerr.KindUnavailable},
		{name: "header capability", err: nats.ErrHeadersNotSupported, kind: xerr.KindUnavailable},
		{name: "authentication expired", err: nats.ErrAuthExpired, kind: xerr.KindUnauthorized},
		{name: "permission denied", err: nats.ErrPermissionViolation, kind: xerr.KindForbidden},
		{name: "server limit", err: nats.ErrMaxConnectionsExceeded, kind: xerr.KindTooManyRequests},
		{name: "legacy stream missing", err: nats.ErrStreamNotFound, kind: xerr.KindNotFound},
		{name: "modern object missing", err: jetstream.ErrObjectNotFound, kind: xerr.KindNotFound},
		{name: "legacy key conflict", err: nats.ErrKeyExists, kind: xerr.KindConflict},
		{name: "modern consumer conflict", err: jetstream.ErrConsumerExists, kind: xerr.KindConflict},
		{name: "JetStream API 400", err: jetstream.ErrStreamMessageExceedsMaximum, kind: xerr.KindBadRequest},
		{name: "JetStream unavailable", err: jetstream.ErrJetStreamNotEnabled, kind: xerr.KindUnavailable},
		{name: "unknown", err: errors.New("unknown failure"), kind: xerr.KindInternal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			wrapped := fmt.Errorf("upstream: %w", tt.err)
			mapped := tnats.MapError(wrapped)
			if got := xerr.KindFrom(mapped); got != tt.kind {
				t.Fatalf("MapError kind = %s, want %s (error: %v)", got, tt.kind, mapped)
			}
			if !errors.Is(mapped, tt.err) {
				t.Fatalf("mapped error %v does not preserve cause %v", mapped, tt.err)
			}
		})
	}
}

func TestMapErrorPreservesAppErrorAndNil(t *testing.T) {
	t.Parallel()

	appErr := xerr.BadRequest("keep original", nats.ErrAuthorization)
	if got := tnats.MapError(appErr); !errors.Is(got, appErr) {
		t.Fatalf("MapError changed existing AppError: got %p, want %p", got, appErr)
	}
	if got := tnats.MapError(nil); got != nil {
		t.Fatalf("MapError(nil) = %v, want nil", got)
	}
}
