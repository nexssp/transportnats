package tnats

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
)

func (t *Transport) subscribeDurable(
	ctx context.Context, nc *nats.Conn, js nats.JetStreamContext,
	ex action.Executable, b DurableBinding, meta *action.Meta,
) error {
	if err := validateDurableBinding(b); err != nil {
		return err
	}

	if err := t.ensureDurableInfra(js, b); err != nil {
		return err
	}

	sub, err := js.PullSubscribe(b.Subject, b.Durable,
		nats.BindStream(b.Stream),
		nats.AckExplicit(),
		nats.AckWait(b.AckWait),
		nats.MaxDeliver(b.MaxDeliver),
		nats.MaxAckPending(b.MaxAckPending),
	)
	if err != nil {
		return MapError(err)
	}

	t.mu.Lock()
	t.subs = append(t.subs, sub)
	t.mu.Unlock()

	t.workersWg.Add(1)
	go t.runFetchLoop(ctx, nc, sub, func(msg *nats.Msg) {
		t.handleDurableMessage(ctx, js, ex, b, meta, msg)
	})

	return nil
}

func (t *Transport) handleDurableMessage(
	ctx context.Context, js nats.JetStreamContext, ex action.Executable,
	b DurableBinding, meta *action.Meta, msg *nats.Msg,
) {
	reqCtx, scope, release := xctx.NewScope(ctx)
	defer release()

	scope.Endpoint = "nats.durable." + msg.Subject
	reqCtx = extractContextHeaders(reqCtx, msg, scope)

	decoder := func(v any) error {
		if len(msg.Data) == 0 {
			return nil
		}

		return t.codec.Unmarshal(msg.Data, v)
	}

	var execErr error
	if meta != nil && meta.Idempotency.Enabled {
		_, execErr = executeWithIdempotency(reqCtx, t.idemStore, meta.Idempotency, ex, msg, decoder, t.codec)
	} else {
		_, execErr = ex.ExecuteDecoded(reqCtx, decoder)
	}

	if execErr == nil {
		ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultShutdownTimeout)
		defer cancel()

		_ = msg.AckSync(nats.Context(ackCtx)) //nolint:errcheck // ack failure is not recoverable by the handler

		return
	}

	metaInfo, metadataErr := msg.Metadata()
	if metadataErr != nil || b.MaxDeliver <= 0 || metaInfo.NumDelivered < uint64(b.MaxDeliver) {
		delay := b.RetryDelay
		if retry, ok := execErr.(interface{ RetryAfter() time.Duration }); ok && retry.RetryAfter() > 0 {
			delay = retry.RetryAfter()
		}

		_ = msg.NakWithDelay(delay) //nolint:errcheck // NAck failure falls back to AckWait redelivery

		return
	}

	headers := make(map[string]string, len(msg.Header))
	for key, values := range msg.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	dead := DeadLetter{
		Stream:     b.Stream,
		Subject:    msg.Subject,
		Durable:    b.Durable,
		Deliveries: metaInfo.NumDelivered,
		FailedAt:   time.Now().UTC(),
		Error:      execErr.Error(),
		Payload:    append([]byte(nil), msg.Data...),
		Headers:    headers,
	}

	payload, marshalErr := json.Marshal(dead)
	if marshalErr != nil {
		_ = msg.NakWithDelay(b.RetryDelay) //nolint:errcheck // NAck failure falls back to AckWait redelivery

		return
	}

	dlqMsg := nats.NewMsg(b.DeadLetterSubject)

	dlqMsg.Data = payload
	if _, publishErr := js.PublishMsg(dlqMsg); publishErr != nil {
		_ = msg.NakWithDelay(b.RetryDelay) //nolint:errcheck // NAck failure falls back to AckWait redelivery

		return
	}

	_ = msg.Term() //nolint:errcheck // terminal state; no retry possible
}

func validateDurableBinding(b DurableBinding) error {
	if b.Stream == "" || b.Subject == "" || b.Durable == "" || b.DeadLetterSubject == "" {
		return xerr.BadRequest("durable work requires stream, subject, durable consumer, and dead-letter subject")
	}

	if b.MaxDeliver <= 0 || b.AckWait <= 0 || b.RetryDelay <= 0 || b.MaxAckPending <= 0 {
		return xerr.BadRequest("durable delivery policy parameters must be positive")
	}

	return nil
}
