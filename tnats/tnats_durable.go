package tnats

import (
	"context"
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
		nats.MaxDeliver(-1), // Application code enforces MaxDeliver; NATS must allow DLQ retries.
		nats.MaxAckPending(b.MaxAckPending),
	)
	if err != nil {
		return MapError(err)
	}
	if err := ensureUnlimitedConsumerDeliveries(js, b.Stream, b.Durable); err != nil {
		if unsubscribeErr := sub.Unsubscribe(); unsubscribeErr != nil {
			t.log.Warn("nats_pull_subscription_unsubscribe_failed", "error", MapError(unsubscribeErr))
		}
		return err
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

	metaInfo, metadataErr := msg.Metadata()
	if metadataErr != nil {
		t.log.Warn("durable_metadata_failed", "subject", msg.Subject, "error", MapError(metadataErr))
		ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultShutdownTimeout)
		delay := durableRetryDelay(MapError(metadataErr), b.RetryDelay)
		if err := msg.NakWithDelay(delay, nats.Context(ackCtx)); err != nil {
			t.log.Error("durable_nak_failed", "subject", msg.Subject, "delay", delay, "error", MapError(err))
		}
		cancel()
		return
	}
	deliveries := metaInfo.NumDelivered
	if maxDeliverExceeded(deliveries, b.MaxDeliver) {
		ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultShutdownTimeout)
		defer cancel()
		exceededErr := xerr.Internal("durable delivery limit exceeded; retrying dead-letter handling")
		binding := ConsumerBinding{
			Stream:            b.Stream,
			Durable:           b.Durable,
			DeadLetterSubject: b.DeadLetterSubject,
		}
		if err := t.dlqConsumerMessage(ackCtx, js, binding, msg, exceededErr, deliveries, true); err != nil {
			t.log.Error("durable_dead_letter_failed", "subject", msg.Subject, "error", err)
			if nakErr := msg.NakWithDelay(durableRetryDelay(err, b.RetryDelay), nats.Context(ackCtx)); nakErr != nil {
				t.log.Error("durable_nak_failed", "subject", msg.Subject, "error", MapError(nakErr))
			}
		}
		return
	}

	var execErr error
	if meta != nil && meta.Idempotency.Enabled {
		_, execErr = executeWithIdempotency(reqCtx, t.idemStore, meta.Idempotency, ex, msg, decoder, t.codec)
	} else {
		_, execErr = ex.ExecuteDecoded(reqCtx, decoder)
	}

	ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultShutdownTimeout)
	defer cancel()
	if execErr == nil {
		if err := msg.AckSync(nats.Context(ackCtx)); err != nil {
			t.log.Error("durable_ack_failed", "subject", msg.Subject, "error", MapError(err))
		}
		return
	}

	t.log.Error("durable_action_failed", "subject", msg.Subject, "error", execErr)
	if !maxDeliverReached(deliveries, b.MaxDeliver) {
		delay := durableRetryDelay(execErr, b.RetryDelay)
		if err := msg.NakWithDelay(delay, nats.Context(ackCtx)); err != nil {
			t.log.Error("durable_nak_failed", "subject", msg.Subject, "delay", delay, "error", MapError(err))
		}
		return
	}

	binding := ConsumerBinding{
		Stream:            b.Stream,
		Durable:           b.Durable,
		DeadLetterSubject: b.DeadLetterSubject,
	}
	if err := t.dlqConsumerMessage(ackCtx, js, binding, msg, execErr, metaInfo.NumDelivered, true); err != nil {
		t.log.Error("durable_dead_letter_failed", "subject", msg.Subject, "error", err)
		if nakErr := msg.NakWithDelay(b.RetryDelay, nats.Context(ackCtx)); nakErr != nil {
			t.log.Error("durable_nak_failed", "subject", msg.Subject, "error", MapError(nakErr))
		}
	}
}

func durableRetryDelay(err error, fallback time.Duration) time.Duration {
	if delay := retryAfter(err); delay > 0 {
		return delay
	}
	return fallback
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
