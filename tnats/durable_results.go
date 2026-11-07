package tnats

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

type durableResultEnvelope struct {
	JobID  string              `json:"job_id"`
	Status string              `json:"status"`
	Result any                 `json:"result"`
	Error  *xerr.ErrorResponse `json:"error"`
}

const (
	durableResultStatusSucceeded     = "succeeded"
	durableResultStatusFailed        = "failed"
	durableResultStatusIndeterminate = "indeterminate"
)

func (t *Transport) handleDurableResultMessage(
	reqCtx context.Context, js nats.JetStreamContext, ex action.Executable,
	b DurableBinding, meta *action.Meta, msg *nats.Msg,
	jobID string, identityErr error, deliveries uint64, decoder func(any) error,
) {
	if _, ok := t.idemStore.(*KVIdempotencyCoordinator); !ok {
		t.nakDurableResult(reqCtx, b, msg, xerr.Unavailable("durable results require a pre-provisioned JetStream KV idempotency bucket"))
		return
	}
	if identityErr != nil {
		t.deadLetterDurableResult(reqCtx, js, b, msg, identityErr, deliveries)
		return
	}
	coordinator, ok := t.idemStore.(action.IdempotencyCoordinator)
	if !ok {
		t.nakDurableResult(reqCtx, b, msg, xerr.Unavailable("configured idempotency store does not support durable result claims"))
		return
	}
	if jobID == "" {
		t.deadLetterDurableResult(reqCtx, js, b, msg, xerr.BadRequest("durable result job ID is missing"), deliveries)
		return
	}

	storageKey := durableResultStorageKey(jobID)
	requestHash := msgHash(msg)
	leaseTTL := action.DefaultIdempotencyLeaseTTL
	claim, err := coordinator.Claim(reqCtx, storageKey, requestHash, leaseTTL)
	if err != nil {
		if errors.Is(err, errCorruptIdempotencyLedger) {
			// Corrupt state cannot be repaired by retrying the same read.
			t.deadLetterDurableResult(reqCtx, js, b, msg, err, deliveries)
			return
		}
		// Preserve the source while transient KV or transport failures are repaired.
		t.nakDurableResult(reqCtx, b, msg, err)
		return
	}
	if t.handleDurableResultClaim(reqCtx, js, b, msg, jobID, claim, deliveries) {
		return
	}
	t.recordAcquiredDurableResult(
		reqCtx, js, b, coordinator, claim, storageKey, requestHash, leaseTTL,
		meta, ex, msg, jobID, identityErr, deliveries, decoder,
	)
}

func (t *Transport) handleDurableResultClaim(
	ctx context.Context, js nats.JetStreamContext, b DurableBinding, msg *nats.Msg,
	jobID string, claim action.IdempotencyClaim, deliveries uint64,
) bool {
	switch claim.State {
	case action.IdempotencyClaimCompleted:
		t.finishStoredDurableResult(ctx, js, b, msg, jobID, claim.Entry.Body, deliveries)
		return true
	case action.IdempotencyClaimConflict:
		t.deadLetterDurableResult(ctx, js, b, msg,
			xerr.Conflict("durable job_id was already used with a different payload"), deliveries)
		return true
	case action.IdempotencyClaimInProgress:
		t.nakDurableResult(ctx, b, msg, xerr.Unavailable("durable result is being recorded by another delivery"))
		return true
	case action.IdempotencyClaimAcquired:
		return false
	default:
		t.nakDurableResult(ctx, b, msg, xerr.Internal("invalid durable result claim state"))
		return true
	}
}

func (t *Transport) recordAcquiredDurableResult(
	ctx context.Context, js nats.JetStreamContext, b DurableBinding,
	coordinator action.IdempotencyCoordinator, claim action.IdempotencyClaim,
	storageKey, requestHash string, leaseTTL time.Duration, meta *action.Meta,
	ex action.Executable, msg *nats.Msg, jobID string, identityErr error,
	deliveries uint64, decoder func(any) error,
) {
	result, status, proceed, execErr := t.executeClaimedDurableResult(
		ctx, b, msg, meta, ex, coordinator, storageKey, claim, leaseTTL,
		identityErr, deliveries, decoder,
	)
	if !proceed {
		return
	}

	payload, marshalErr := marshalDurableResult(jobID, status, result, execErr)
	if marshalErr != nil && execErr == nil {
		execErr = xerr.Internal("failed to encode durable result", marshalErr)
		status = durableResultStatusFailed
		payload, marshalErr = marshalDurableResult(jobID, status, nil, execErr)
	}
	if marshalErr != nil {
		t.nakDurableResult(ctx, b, msg, marshalErr)
		return
	}

	entry := action.IdempotencyEntry{
		Status:      200,
		Body:        payload,
		StoredAt:    time.Now().UTC(),
		RequestHash: requestHash,
	}
	ackCtx, cancelAck := durableResultAckContext(ctx)
	completeErr := coordinator.Complete(ackCtx, storageKey, claim.Token, entry, 0)
	cancelAck()
	if completeErr != nil {
		t.nakDurableResult(ctx, b, msg, completeErr)
		return
	}
	t.finishStoredDurableResult(ctx, js, b, msg, jobID, payload, deliveries)
}

func (t *Transport) executeClaimedDurableResult(
	ctx context.Context, b DurableBinding, msg *nats.Msg, meta *action.Meta,
	ex action.Executable, coordinator action.IdempotencyCoordinator,
	storageKey string, claim action.IdempotencyClaim, leaseTTL time.Duration,
	identityErr error, deliveries uint64, decoder func(any) error,
) (result any, status string, proceed bool, execErr error) {
	switch {
	case identityErr != nil:
		execErr = identityErr
	case maxDeliverExceeded(deliveries, b.MaxDeliver):
		status = durableResultStatusIndeterminate
		execErr = xerr.Internal("durable delivery limit exceeded before an outcome was durably recorded; execution outcome is indeterminate")
	default:
		result, execErr = t.executeDurableResultWithLease(
			ctx, coordinator, storageKey, claim.Token, leaseTTL, meta, ex, msg, decoder,
		)
	}

	if status == "" && execErr != nil && ctx.Err() != nil {
		t.releaseDurableResultClaim(ctx, coordinator, storageKey, claim, msg)
		t.nakDurableResult(ctx, b, msg, ctx.Err())
		return nil, "", false, ctx.Err()
	}
	if status == "" && execErr != nil && !maxDeliverReached(deliveries, b.MaxDeliver) {
		t.releaseDurableResultClaim(ctx, coordinator, storageKey, claim, msg)
		t.nakDurableResult(ctx, b, msg, execErr)
		return nil, "", false, execErr
	}
	if status == "" {
		status = durableResultStatusSucceeded
		if execErr != nil {
			status = durableResultStatusFailed
		}
	}
	return result, status, true, execErr
}

func (t *Transport) executeDurableResultWithLease(
	ctx context.Context, coordinator action.IdempotencyCoordinator,
	storageKey, token string, leaseTTL time.Duration, meta *action.Meta,
	ex action.Executable, msg *nats.Msg, decoder func(any) error,
) (any, error) {
	actionCtx, cancelAction := context.WithCancel(ctx)
	renewalErrors, stopRenewal := startCoordinatorLeaseRenewal(
		ctx, coordinator, storageKey, token, leaseTTL, cancelAction,
	)
	result, execErr := t.executeDurableResultAction(actionCtx, meta, ex, msg, decoder)
	stopRenewal()
	cancelAction()
	select {
	case renewalErr := <-renewalErrors:
		execErr = renewalErr
	default:
	}
	return result, execErr
}

func (t *Transport) releaseDurableResultClaim(
	ctx context.Context, coordinator action.IdempotencyCoordinator,
	storageKey string, claim action.IdempotencyClaim, msg *nats.Msg,
) {
	releaseCtx, cancelRelease := durableResultAckContext(ctx)
	releaseErr := coordinator.Release(releaseCtx, storageKey, claim.Token)
	cancelRelease()
	if releaseErr != nil {
		t.log.Warn("durable_result_claim_release_failed", "subject", msg.Subject, "error", MapError(releaseErr))
	}
}

func (t *Transport) executeDurableResultAction(
	ctx context.Context, meta *action.Meta, ex action.Executable,
	msg *nats.Msg, decoder func(any) error,
) (any, error) {
	if meta == nil || !meta.Idempotency.Enabled {
		return ex.ExecuteDecoded(ctx, decoder)
	}
	encoded, err := executeWithIdempotency(ctx, t.idemStore, meta.Idempotency, ex, msg, decoder, t.codec)
	if err != nil {
		return nil, err
	}
	var result any
	if err := t.codec.Unmarshal(encoded, &result); err != nil {
		return nil, xerr.Internal("failed to decode idempotent durable result", err)
	}
	return result, nil
}

func (t *Transport) finishStoredDurableResult(
	ctx context.Context, js nats.JetStreamContext, b DurableBinding, source *nats.Msg,
	jobID string, payload []byte, deliveries uint64,
) {
	ackCtx, cancelAck := durableResultAckContext(ctx)
	defer cancelAck()
	envelope, err := decodeDurableResult(payload, jobID)
	if err != nil {
		// A corrupt outcome cannot be replayed or replaced without risking lost data.
		t.deadLetterDurableResult(ctx, js, b, source, err, deliveries)
		return
	}
	if err := publishDurableResult(ackCtx, js, b, jobID, payload); err != nil {
		t.nakDurableResult(ctx, b, source, err)
		return
	}

	if envelope.Status == durableResultStatusSucceeded {
		if err := source.AckSync(nats.Context(ackCtx)); err != nil {
			t.log.Error("durable_ack_failed", "subject", source.Subject, "error", MapError(err))
		}
		return
	}
	t.deadLetterDurableResult(ctx, js, b, source, durableResultError(envelope), deliveries)
}

func (t *Transport) deadLetterDurableResult(
	ctx context.Context, js nats.JetStreamContext, b DurableBinding, msg *nats.Msg,
	execErr error, deliveries uint64,
) {
	ackCtx, cancelAck := durableResultAckContext(ctx)
	defer cancelAck()
	binding := ConsumerBinding{
		Stream:            b.Stream,
		Durable:           b.Durable,
		DeadLetterSubject: b.DeadLetterSubject,
	}
	if err := t.dlqConsumerMessage(ackCtx, js, binding, msg, execErr, deliveries, true); err != nil {
		t.log.Error("durable_dead_letter_failed", "subject", msg.Subject, "error", err)
		t.nakDurableResult(ctx, b, msg, err)
	}
}

func (t *Transport) nakDurableResult(ctx context.Context, b DurableBinding, msg *nats.Msg, cause error) {
	t.log.Error("durable_result_retry", "subject", msg.Subject, "error", cause)
	ackCtx, cancelAck := durableResultAckContext(ctx)
	defer cancelAck()
	if err := msg.NakWithDelay(durableRetryDelay(cause, b.RetryDelay), nats.Context(ackCtx)); err != nil {
		t.log.Error("durable_nak_failed", "subject", msg.Subject, "error", MapError(err))
	}
}

func durableResultAckContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), defaultShutdownTimeout)
}

func marshalDurableResult(jobID, status string, result any, execErr error) ([]byte, error) {
	envelope := durableResultEnvelope{JobID: jobID, Status: status, Result: result}
	if execErr != nil {
		envelope.Result = nil
		publicErr := xerr.From(execErr).Public(jobID)
		envelope.Error = &publicErr
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshal durable result envelope: %w", err)
	}
	return payload, nil
}

func decodeDurableResult(payload []byte, jobID string) (durableResultEnvelope, error) {
	var envelope durableResultEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return durableResultEnvelope{}, xerr.Internal("corrupted durable result in KV", err)
	}
	if envelope.JobID != jobID || (envelope.Status != durableResultStatusSucceeded &&
		envelope.Status != durableResultStatusFailed && envelope.Status != durableResultStatusIndeterminate) {
		return durableResultEnvelope{}, xerr.Internal("corrupted durable result in KV")
	}
	if (envelope.Status == durableResultStatusSucceeded) == (envelope.Error != nil) {
		return durableResultEnvelope{}, xerr.Internal("corrupted durable result in KV")
	}
	return envelope, nil
}

func durableResultError(envelope durableResultEnvelope) error {
	if envelope.Error != nil {
		if appErr, ok := xerr.FromPublic(*envelope.Error); ok {
			return appErr
		}
		if envelope.Error.Message != "" {
			return fmt.Errorf("%s: %s", envelope.Error.Error, envelope.Error.Message)
		}
	}
	return xerr.Internal("durable delivery outcome is indeterminate")
}

func durableResultStorageKey(jobID string) string {
	hash := sha256.Sum256([]byte(jobID))
	return "durable-result." + hex.EncodeToString(hash[:])
}

func durableResultMessageID(jobID string) string {
	hash := sha256.Sum256([]byte(jobID))
	return "result-" + hex.EncodeToString(hash[:16])
}

func publishDurableResult(ctx context.Context, js nats.JetStreamContext, b DurableBinding, jobID string, payload []byte) error {
	resultMsg := nats.NewMsg(b.ResultSubject)
	resultMsg.Data = payload
	if _, err := js.PublishMsg(resultMsg,
		nats.MsgId(durableResultMessageID(jobID)),
		nats.ExpectStream(b.ResultStream),
		nats.Context(ctx),
	); err != nil {
		return MapError(err)
	}
	return nil
}
