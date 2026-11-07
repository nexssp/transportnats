package tnats

import (
	"context"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/transport/codec"
	"github.com/nexssp/transport/identity"
)

const idempotencyKeyHeader = "Idempotency-Key"

func applyDurableDeliveryIdentity(
	ctx context.Context, msg *nats.Msg, binding DurableBinding, payloadCodec codec.Codec,
) (context.Context, error) {
	field := binding.DeliveryIdentityField
	if field == "" {
		return ctx, nil
	}

	if payloadCodec == nil {
		payloadCodec = codec.Default
	}
	var payload map[string]any
	if err := payloadCodec.Unmarshal(msg.Data, &payload); err != nil {
		return ctx, xerr.BadRequest("durable delivery identity requires an object payload", err)
	}

	requestID, ok := payload[field].(string)
	if !ok {
		return ctx, xerr.BadRequest("durable payload identity field " + field + " must be a string")
	}
	if requestID == "" || strings.TrimSpace(requestID) != requestID || len(requestID) > 256 || strings.ContainsRune(requestID, '\x00') {
		return ctx, xerr.BadRequest("durable payload identity field " + field + " must be a non-empty ID of at most 256 bytes without NUL or surrounding whitespace")
	}

	if msg.Header == nil {
		msg.Header = make(nats.Header)
	}
	if key := msg.Header.Get(idempotencyKeyHeader); key != "" && key != requestID {
		return ctx, xerr.Conflict("Idempotency-Key must match durable payload identity field " + field)
	}

	executionID := identity.NewID()
	msg.Header.Set(identity.HeaderRequestID, requestID)
	msg.Header.Set(idempotencyKeyHeader, requestID)
	msg.Header.Set(identity.HeaderExecutionID, executionID)
	ctx = xctx.WithRequestID(ctx, requestID)
	ctx = xctx.WithExecutionID(ctx, executionID)
	ctx = xctx.WithRootExecutionID(ctx, executionID)
	return ctx, nil
}
