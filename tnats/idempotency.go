package tnats

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/transport/codec"
)

const idempotencyFinalizeTimeout = 5 * time.Second

// msgHash generates a 64-character hex hash while keeping zero heap allocations
// for the common case of small subjects and payloads.
func msgHash(msg *nats.Msg) string {
	var subjectLen [8]byte
	binary.BigEndian.PutUint64(subjectLen[:], uint64(len(msg.Subject)))
	totalLen := len(subjectLen) + len(msg.Subject) + len(msg.Data)
	if totalLen <= 512 {
		var stackBuf [512]byte
		copy(stackBuf[:], subjectLen[:])
		copy(stackBuf[len(subjectLen):], msg.Subject)
		copy(stackBuf[len(subjectLen)+len(msg.Subject):], msg.Data)
		sum := sha256.Sum256(stackBuf[:totalLen])

		return hex.EncodeToString(sum[:])
	}

	h := sha256.New()
	_, _ = h.Write(subjectLen[:])       // SHA-256 writes never return an error.
	_, _ = h.Write([]byte(msg.Subject)) // SHA-256 writes never return an error.
	_, _ = h.Write(msg.Data)            // SHA-256 writes never return an error.

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
	leaseTTL := cfg.EffectiveLeaseTTL()
	claim, err := coordinator.Claim(ctx, key, reqHash, leaseTTL)
	if err != nil {
		return encodeError(c, err)
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
		// Own the execution right.
	default:
		return encodeError(c, xerr.Internal("invalid idempotency claim state"))
	}

	completed := false
	defer func() {
		if !completed {
			releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), idempotencyFinalizeTimeout)
			defer cancel()
			if releaseErr := coordinator.Release(releaseCtx, key, claim.Token); releaseErr != nil {
				slog.ErrorContext(ctx, "idempotency_claim_release_failed", "error", releaseErr)
			}
		}
	}()

	actionCtx, cancelAction := context.WithCancel(ctx)
	renewalErrors, stopRenewal := startCoordinatorLeaseRenewal(
		ctx, coordinator, key, claim.Token, leaseTTL, cancelAction,
	)
	defer cancelAction()
	defer stopRenewal()

	res, execErr := ex.ExecuteDecoded(actionCtx, decoder)
	stopRenewal()
	cancelAction()
	select {
	case renewalErr := <-renewalErrors:
		return encodeError(c, renewalErr)
	default:
	}
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

func startCoordinatorLeaseRenewal(
	ctx context.Context, coordinator action.IdempotencyCoordinator, key, token string, leaseTTL time.Duration,
	cancelAction context.CancelFunc,
) (renewalErrs <-chan error, stopRenewal func()) {
	renewer, ok := coordinator.(action.IdempotencyLeaseRenewer)
	if !ok || leaseTTL <= 0 {
		return nil, func() {}
	}

	interval := leaseTTL / 3
	if interval <= 0 {
		interval = leaseTTL
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	renewalErrors := make(chan error, 1)
	var stateMu sync.Mutex
	stopped := false
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				renewCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), interval)
				renewErr := renewer.RenewClaim(renewCtx, key, token, leaseTTL)
				cancel()
				if renewErr != nil {
					stateMu.Lock()
					if !stopped {
						mappedErr := MapError(renewErr)
						renewalErrors <- mappedErr
						cancelAction()
						slog.WarnContext(ctx, "idempotency_lease_renewal_failed", "key", key, "error", mappedErr)
					}
					stateMu.Unlock()
					return
				}
			}
		}
	}()

	var stopOnce sync.Once
	return renewalErrors, func() {
		stopOnce.Do(func() {
			stateMu.Lock()
			stopped = true
			stateMu.Unlock()
			close(stop)
			<-done
		})
	}
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

	encoded, marshalErr := c.Marshal(appErr.Public(""))
	if marshalErr == nil {
		return encoded, err
	}
	slog.Error("nats_error_response_marshal_failed", "error", marshalErr)
	encoded, fallbackErr := json.Marshal(appErr.Public(""))
	if fallbackErr != nil {
		slog.Error("nats_error_response_fallback_failed", "error", fallbackErr)
		encoded = []byte(`{"error":"internal","message":"failed to encode error response"}`)
	}
	return encoded, err
}
