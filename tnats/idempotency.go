package tnats

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/transport/codec"
)

const idempotencyFinalizeTimeout = 5 * time.Second

// msgHash generuje 64-bajtowy hex hash z zachowaniem 0 alokacji sterty przy hashowaniu.
func msgHash(msg *nats.Msg) string {
	// Przygotowanie bufora na stosie
	totalLen := len(msg.Subject) + len(msg.Data)
	if totalLen <= 512 {
		var stackBuf [512]byte
		copy(stackBuf[:], msg.Subject)
		copy(stackBuf[len(msg.Subject):], msg.Data)
		sum := sha256.Sum256(stackBuf[:totalLen])

		return hex.EncodeToString(sum[:])
	}

	h := sha256.New()
	_, _ = h.Write([]byte(msg.Subject))
	_, _ = h.Write(msg.Data)

	var sumBuf [32]byte

	return hex.EncodeToString(h.Sum(sumBuf[:0]))
}

func executeWithIdempotency(
	ctx context.Context,
	store action.IdempotencyStore,
	cfg action.IdempotencyConfig,
	ex action.Executable,
	msg *nats.Msg,
	decoder func(any) error,
	c codec.Codec,
) ([]byte, error) {
	key := ""
	if msg.Header != nil {
		key = msg.Header.Get(cfg.Header())
	}

	if key == "" && cfg.KeyFunc != nil {
		key = cfg.KeyFunc(msg.Data)
	}

	if key == "" {
		key = msgHash(msg)
	}

	reqHash := msgHash(msg)

	if coordinator, ok := store.(action.IdempotencyCoordinator); ok {
		return executeWithCoordinator(ctx, coordinator, cfg, ex, key, reqHash, decoder, c)
	}

	return executeWithLegacyStore(ctx, store, cfg, ex, key, reqHash, decoder, c)
}

func executeWithCoordinator(
	ctx context.Context,
	coordinator action.IdempotencyCoordinator,
	cfg action.IdempotencyConfig,
	ex action.Executable,
	key, reqHash string,
	decoder func(any) error,
	c codec.Codec,
) ([]byte, error) {
	claim, err := coordinator.Claim(ctx, key, reqHash, cfg.EffectiveLeaseTTL())
	if err != nil {
		return encodeError(c, xerr.Unavailable("idempotency coordination unavailable", err))
	}

	switch claim.State {
	case action.IdempotencyClaimCompleted:
		if claim.Entry.RequestHash != "" && claim.Entry.RequestHash != reqHash {
			return encodeError(c, xerr.Conflict("idempotency key already used with a different payload"))
		}

		return claim.Entry.Body, nil
	case action.IdempotencyClaimConflict:
		return encodeError(c, xerr.Conflict("idempotency key already used with a different payload"))
	case action.IdempotencyClaimInProgress:
		return encodeError(c, xerr.Unavailable("an identical request is currently in progress"))
	case action.IdempotencyClaimAcquired:
		// Prawo do wykonania akcji
	default:
		return encodeError(c, xerr.Internal("invalid idempotency claim state"))
	}

	completed := false
	defer func() {
		if !completed {
			_ = coordinator.Release(ctx, key, claim.Token)
		}
	}()

	res, execErr := ex.ExecuteDecoded(ctx, decoder)
	if execErr != nil {
		return encodeError(c, execErr)
	}

	replyBytes, err := c.Marshal(res)
	if err != nil {
		return encodeError(c, xerr.Internal("failed to marshal idempotent response", err))
	}

	entry := action.IdempotencyEntry{
		Status:      200,
		Body:        replyBytes,
		StoredAt:    time.Now().UTC(),
		RequestHash: reqHash,
	}

	finalizeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), idempotencyFinalizeTimeout)
	defer cancel()

	if err := coordinator.Complete(finalizeCtx, key, claim.Token, entry, cfg.TTL); err != nil {
		return encodeError(c, xerr.Conflict("idempotency outcome is indeterminate", err))
	}

	completed = true

	return replyBytes, nil
}

func executeWithLegacyStore(
	ctx context.Context,
	store action.IdempotencyStore,
	cfg action.IdempotencyConfig,
	ex action.Executable,
	key, reqHash string,
	decoder func(any) error,
	c codec.Codec,
) ([]byte, error) {
	if entry, ok := store.Get(ctx, key); ok {
		if entry.RequestHash != "" && entry.RequestHash != reqHash {
			return encodeError(c, xerr.Conflict("idempotency key already used with a different payload"))
		}

		return entry.Body, nil
	}

	res, execErr := ex.ExecuteDecoded(ctx, decoder)
	if execErr != nil {
		return encodeError(c, execErr)
	}

	replyBytes, err := c.Marshal(res)
	if err != nil {
		return encodeError(c, xerr.Internal("failed to marshal idempotent response", err))
	}

	store.Set(ctx, key, action.IdempotencyEntry{
		Status:      200,
		Body:        replyBytes,
		StoredAt:    time.Now().UTC(),
		RequestHash: reqHash,
	}, cfg.TTL)

	return replyBytes, nil
}

func encodeError(c codec.Codec, err error) ([]byte, error) {
	appErr := xerr.From(err)

	if c == nil {
		c = codec.Default
	}

	encoded, _ := c.Marshal(appErr.Public(""))

	return encoded, err
}
