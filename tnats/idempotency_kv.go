package tnats

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

const (
	defaultIdempotencyBucket = "NEXSS_IDEMPOTENCY"
	claimPrefix              = "claim."
	entryPrefix              = "entry."
)

var errCorruptIdempotencyLedger = errors.New("corrupt idempotency ledger")

type kvClaimPayload struct {
	Token       string `json:"token"`
	RequestHash string `json:"req_hash"`
	ExpiresAt   int64  `json:"exp"`
}

func decodeKVClaim(data []byte) (kvClaimPayload, error) {
	var claim kvClaimPayload
	if err := json.Unmarshal(data, &claim); err != nil {
		return kvClaimPayload{}, xerr.Internal("corrupted idempotency claim in KV", errors.Join(errCorruptIdempotencyLedger, err))
	}
	if claim.Token == "" || claim.ExpiresAt <= 0 {
		return kvClaimPayload{}, xerr.Internal("corrupted idempotency claim in KV", errCorruptIdempotencyLedger)
	}
	return claim, nil
}

type kvEntryPayload struct {
	Entry     *action.IdempotencyEntry `json:"entry"`
	ExpiresAt int64                    `json:"exp"`
}

// KVIdempotencyCoordinator implements action.IdempotencyCoordinator backed by NATS JetStream KV.
type KVIdempotencyCoordinator struct {
	kv nats.KeyValue
}

var (
	_ action.IdempotencyCoordinator  = (*KVIdempotencyCoordinator)(nil)
	_ action.IdempotencyLeaseRenewer = (*KVIdempotencyCoordinator)(nil)
)

// NewKVIdempotencyCoordinator creates a distributed idempotency coordinator backed by JetStream KV.
func NewKVIdempotencyCoordinator(js nats.JetStreamContext, bucketName ...string) (*KVIdempotencyCoordinator, error) {
	if js == nil {
		return nil, xerr.BadRequest("idempotency JetStream context is required")
	}
	if len(bucketName) > 1 {
		return nil, xerr.BadRequest("at most one idempotency bucket name may be specified")
	}

	name := defaultIdempotencyBucket
	if len(bucketName) == 1 && bucketName[0] != "" {
		name = bucketName[0]
	}

	kv, err := js.KeyValue(name)
	if errors.Is(err, nats.ErrBucketNotFound) {
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:      name,
			Description: "Nexss Distributed Idempotency Ledger",
			History:     1,
			Storage:     nats.FileStorage,
		})
	}
	if err != nil {
		return nil, MapError(err)
	}
	if err := removeIdempotencyBucketMaxAge(js, kv, name); err != nil {
		return nil, err
	}

	return &KVIdempotencyCoordinator{kv: kv}, nil
}

func removeIdempotencyBucketMaxAge(js nats.JetStreamContext, kv nats.KeyValue, name string) error {
	status, err := kv.Status()
	if err != nil {
		return MapError(err)
	}
	if status.TTL() <= 0 {
		return nil
	}

	streamInfo, err := js.StreamInfo("KV_" + name)
	if err != nil {
		return MapError(err)
	}
	streamConfig := streamInfo.Config
	streamConfig.MaxAge = 0
	if _, updateErr := js.UpdateStream(&streamConfig); updateErr != nil {
		return MapError(updateErr)
	}
	status, err = kv.Status()
	if err != nil {
		return MapError(err)
	}
	if status.TTL() > 0 {
		return xerr.Internal("idempotency KV bucket still has a physical TTL")
	}
	return nil
}

// Get retrieves a stored response for a completed request.
func (c *KVIdempotencyCoordinator) Get(ctx context.Context, key string) (action.IdempotencyEntry, bool) {
	ctx = idempotencyContext(ctx)
	entry, found, err := c.loadEntry(ctx, key)
	if err != nil {
		slog.WarnContext(ctx, "idempotency_kv_read_failed", "error", err)
		return action.IdempotencyEntry{}, true
	}
	return entry, found
}

func (c *KVIdempotencyCoordinator) loadEntry(
	ctx context.Context, key string,
) (action.IdempotencyEntry, bool, error) {
	if err := ctx.Err(); err != nil {
		return action.IdempotencyEntry{}, false, err
	}
	if c == nil || c.kv == nil {
		return action.IdempotencyEntry{}, false, xerr.Unavailable("idempotency KV coordinator is not initialized")
	}
	if key == "" {
		return action.IdempotencyEntry{}, false, xerr.BadRequest("idempotency key is required")
	}

	item, err := c.kv.Get(entryPrefix + key)
	if errors.Is(err, nats.ErrKeyNotFound) || errors.Is(err, nats.ErrKeyDeleted) {
		return action.IdempotencyEntry{}, false, nil
	}
	if err != nil {
		return action.IdempotencyEntry{}, false, mapIdempotencyKVError(ctx, err)
	}
	if err := ctx.Err(); err != nil {
		return action.IdempotencyEntry{}, false, err
	}

	return decodeKVEntry(item.Value())
}

// Set implements the Kernel store interface, which has no error return. Failures
// are logged here; distributed claims use Complete, which returns errors.
func (c *KVIdempotencyCoordinator) Set(
	ctx context.Context, key string, entry action.IdempotencyEntry, ttl time.Duration,
) {
	ctx = idempotencyContext(ctx)
	if c == nil || c.kv == nil {
		slog.ErrorContext(ctx, "idempotency_kv_write_failed", "error", "coordinator is not initialized")
		return
	}
	if key == "" {
		slog.ErrorContext(ctx, "idempotency_kv_write_failed", "error", xerr.BadRequest("idempotency key is required"))
		return
	}
	data, err := marshalKVEntry(entry, ttl)
	if err != nil {
		slog.ErrorContext(ctx, "idempotency_kv_write_failed", "error", err)
		return
	}
	if err := ctx.Err(); err != nil {
		slog.WarnContext(ctx, "idempotency_kv_write_canceled", "error", err)
		return
	}
	if _, err := c.kv.Put(entryPrefix+key, data); err != nil {
		slog.ErrorContext(ctx, "idempotency_kv_write_failed", "error", MapError(err))
	}
}

// Claim atomically reserves an idempotency token in NATS KV.
func (c *KVIdempotencyCoordinator) Claim(
	ctx context.Context, key, requestHash string, leaseTTL time.Duration,
) (action.IdempotencyClaim, error) {
	ctx = idempotencyContext(ctx)
	if err := ctx.Err(); err != nil {
		return action.IdempotencyClaim{}, err
	}
	if c == nil || c.kv == nil {
		return action.IdempotencyClaim{}, xerr.Unavailable("idempotency KV coordinator is not initialized")
	}
	if key == "" {
		return action.IdempotencyClaim{}, xerr.BadRequest("idempotency key is required")
	}
	if leaseTTL <= 0 {
		leaseTTL = action.DefaultIdempotencyLeaseTTL
	}

	entry, found, err := c.loadEntry(ctx, key)
	if err != nil {
		return action.IdempotencyClaim{}, err
	}
	if found {
		if entry.RequestHash != "" && entry.RequestHash != requestHash {
			return action.IdempotencyClaim{State: action.IdempotencyClaimConflict}, nil
		}
		return action.IdempotencyClaim{State: action.IdempotencyClaimCompleted, Entry: entry}, nil
	}

	now := time.Now()
	expiresAt, err := idempotencyExpiry(now, leaseTTL)
	if err != nil {
		return action.IdempotencyClaim{}, err
	}
	token, err := newKVClaimToken()
	if err != nil {
		return action.IdempotencyClaim{}, xerr.Internal("failed to generate idempotency claim token", err)
	}
	data, err := json.Marshal(kvClaimPayload{Token: token, RequestHash: requestHash, ExpiresAt: expiresAt})
	if err != nil {
		return action.IdempotencyClaim{}, xerr.Internal("failed to marshal idempotency claim", err)
	}
	if err := ctx.Err(); err != nil {
		return action.IdempotencyClaim{}, err
	}

	return c.createClaim(ctx, claimPrefix+key, requestHash, token, now, data)
}

func (c *KVIdempotencyCoordinator) createClaim(
	ctx context.Context, claimKey, requestHash, token string, now time.Time, data []byte,
) (action.IdempotencyClaim, error) {
	_, createErr := c.kv.Create(claimKey, data)
	if createErr == nil {
		return action.IdempotencyClaim{State: action.IdempotencyClaimAcquired, Token: token}, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return action.IdempotencyClaim{}, ctxErr
	}

	existing, getErr := c.kv.Get(claimKey)
	if errors.Is(getErr, nats.ErrKeyNotFound) || errors.Is(getErr, nats.ErrKeyDeleted) {
		if !errors.Is(createErr, nats.ErrKeyExists) {
			return action.IdempotencyClaim{}, mapIdempotencyKVError(ctx, createErr)
		}
		return c.retryCreateClaim(ctx, claimKey, token, data)
	}
	if getErr != nil {
		return action.IdempotencyClaim{}, mapIdempotencyKVError(ctx, getErr)
	}
	return c.resolveExistingClaim(ctx, claimKey, requestHash, token, now, data, existing)
}

func (c *KVIdempotencyCoordinator) retryCreateClaim(
	ctx context.Context, claimKey, token string, data []byte,
) (action.IdempotencyClaim, error) {
	_, createErr := c.kv.Create(claimKey, data)
	switch {
	case createErr == nil:
		return action.IdempotencyClaim{State: action.IdempotencyClaimAcquired, Token: token}, nil
	case errors.Is(createErr, nats.ErrKeyExists):
		return action.IdempotencyClaim{State: action.IdempotencyClaimInProgress}, nil
	default:
		return action.IdempotencyClaim{}, mapIdempotencyKVError(ctx, createErr)
	}
}

func (c *KVIdempotencyCoordinator) resolveExistingClaim(
	ctx context.Context, claimKey, requestHash, token string, now time.Time, data []byte, existing nats.KeyValueEntry,
) (action.IdempotencyClaim, error) {
	activeClaim, err := decodeKVClaim(existing.Value())
	if err != nil {
		return action.IdempotencyClaim{}, err
	}
	if activeClaim.RequestHash != "" && activeClaim.RequestHash != requestHash {
		return action.IdempotencyClaim{State: action.IdempotencyClaimConflict}, nil
	}
	if now.UnixNano() < activeClaim.ExpiresAt {
		return action.IdempotencyClaim{State: action.IdempotencyClaimInProgress}, nil
	}
	return c.replaceExpiredClaim(ctx, claimKey, token, data, existing.Revision())
}

func (c *KVIdempotencyCoordinator) replaceExpiredClaim(
	ctx context.Context, claimKey, token string, data []byte, revision uint64,
) (action.IdempotencyClaim, error) {
	_, updateErr := c.kv.Update(claimKey, data, revision)
	switch {
	case updateErr == nil:
		return action.IdempotencyClaim{State: action.IdempotencyClaimAcquired, Token: token}, nil
	case errors.Is(updateErr, nats.ErrKeyRevisionMismatch):
		return action.IdempotencyClaim{State: action.IdempotencyClaimInProgress}, nil
	case errors.Is(updateErr, nats.ErrKeyNotFound), errors.Is(updateErr, nats.ErrKeyDeleted):
		if ctxErr := ctx.Err(); ctxErr != nil {
			return action.IdempotencyClaim{}, ctxErr
		}
		return c.retryCreateClaim(ctx, claimKey, token, data)
	default:
		return action.IdempotencyClaim{}, mapIdempotencyKVError(ctx, updateErr)
	}
}

// Complete writes the result before releasing the claim, then removes only the
// exact claim revision owned by token.
func (c *KVIdempotencyCoordinator) Complete(
	ctx context.Context, key, token string, entry action.IdempotencyEntry, ttl time.Duration,
) error {
	ctx = idempotencyContext(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	if c == nil || c.kv == nil {
		return xerr.Unavailable("idempotency KV coordinator is not initialized")
	}
	if key == "" || token == "" {
		return xerr.BadRequest("idempotency key and claim token are required")
	}
	entryData, err := marshalKVEntry(entry, ttl)
	if err != nil {
		return err
	}
	claimRevision, err := c.extendClaimForCompletion(ctx, key, token)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := c.kv.Put(entryPrefix+key, entryData); err != nil {
		return mapIdempotencyKVError(ctx, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.kv.Delete(claimPrefix+key, nats.LastRevision(claimRevision)); err != nil {
		return mapIdempotencyKVError(ctx, err)
	}
	return nil
}

func (c *KVIdempotencyCoordinator) extendClaimForCompletion(
	ctx context.Context, key, token string,
) (uint64, error) {
	claimKey := claimPrefix + key
	item, err := c.kv.Get(claimKey)
	if err != nil {
		return 0, mapIdempotencyKVError(ctx, err)
	}
	activeClaim, err := decodeKVClaim(item.Value())
	if err != nil {
		return 0, err
	}
	if activeClaim.Token != token {
		return 0, xerr.Conflict("idempotency claim is owned by another token")
	}
	now := time.Now()
	if now.UnixNano() >= activeClaim.ExpiresAt {
		return 0, xerr.Conflict("idempotency claim has expired")
	}
	expiresAt, err := idempotencyExpiry(now, action.DefaultIdempotencyLeaseTTL)
	if err != nil {
		return 0, err
	}
	activeClaim.ExpiresAt = expiresAt
	claimData, err := json.Marshal(activeClaim)
	if err != nil {
		return 0, xerr.Internal("failed to marshal idempotency claim", err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return 0, ctxErr
	}
	claimInfo, err := c.kv.Update(claimKey, claimData, item.Revision())
	if err != nil {
		return 0, mapIdempotencyKVError(ctx, err)
	}
	return claimInfo, nil
}

// Release frees the reservation after a failure or panic and returns cleanup errors.
func (c *KVIdempotencyCoordinator) Release(ctx context.Context, key, token string) error {
	ctx = idempotencyContext(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	if c == nil || c.kv == nil {
		return xerr.Unavailable("idempotency KV coordinator is not initialized")
	}
	if key == "" || token == "" {
		return xerr.BadRequest("idempotency key and claim token are required")
	}

	claimKey := claimPrefix + key
	item, err := c.kv.Get(claimKey)
	if errors.Is(err, nats.ErrKeyNotFound) || errors.Is(err, nats.ErrKeyDeleted) {
		return nil
	}
	if err != nil {
		return mapIdempotencyKVError(ctx, err)
	}
	activeClaim, err := decodeKVClaim(item.Value())
	if err != nil {
		return err
	}
	if activeClaim.Token == token {
		err := c.kv.Delete(claimKey, nats.LastRevision(item.Revision()))
		if errors.Is(err, nats.ErrKeyNotFound) || errors.Is(err, nats.ErrKeyDeleted) {
			return nil
		}
		return mapIdempotencyKVError(ctx, err)
	}
	return nil
}

func marshalKVEntry(entry action.IdempotencyEntry, ttl time.Duration) ([]byte, error) {
	expiresAt, err := idempotencyExpiry(time.Now(), ttl)
	if err != nil {
		return nil, err
	}
	return json.Marshal(kvEntryPayload{Entry: &entry, ExpiresAt: expiresAt})
}

func decodeKVEntry(data []byte) (action.IdempotencyEntry, bool, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return action.IdempotencyEntry{}, true, xerr.Internal("corrupted idempotency entry in KV", errors.Join(errCorruptIdempotencyLedger, err))
	}
	if _, wrapped := object["entry"]; wrapped {
		var stored kvEntryPayload
		if err := json.Unmarshal(data, &stored); err != nil {
			return action.IdempotencyEntry{}, true, xerr.Internal("corrupted idempotency entry in KV", errors.Join(errCorruptIdempotencyLedger, err))
		}
		if stored.Entry == nil {
			return action.IdempotencyEntry{}, true, xerr.Internal("corrupted idempotency entry in KV", errCorruptIdempotencyLedger)
		}
		if stored.ExpiresAt != 0 && time.Now().UnixNano() >= stored.ExpiresAt {
			return action.IdempotencyEntry{}, false, nil
		}
		return *stored.Entry, true, nil
	}

	var legacy action.IdempotencyEntry
	if err := json.Unmarshal(data, &legacy); err != nil {
		return action.IdempotencyEntry{}, true, xerr.Internal("corrupted legacy idempotency entry in KV", errors.Join(errCorruptIdempotencyLedger, err))
	}
	return legacy, true, nil
}

func idempotencyExpiry(now time.Time, ttl time.Duration) (int64, error) {
	if ttl <= 0 {
		return 0, nil
	}
	const maxUnixNano = int64(1<<63 - 1)
	if ttl > time.Unix(0, maxUnixNano).Sub(now) {
		return 0, xerr.BadRequest("idempotency TTL is too large")
	}
	return now.Add(ttl).UnixNano(), nil
}

func mapIdempotencyKVError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return MapError(err)
}

func idempotencyContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func newKVClaimToken() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(token[:]), nil
}

// RenewClaim extends the lease of a running distributed idempotent action.
func (c *KVIdempotencyCoordinator) RenewClaim(ctx context.Context, key, token string, leaseTTL time.Duration) error {
	ctx = idempotencyContext(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	if c == nil || c.kv == nil {
		return xerr.Unavailable("idempotency KV coordinator is not initialized")
	}
	if key == "" || token == "" {
		return xerr.BadRequest("idempotency key and claim token are required")
	}
	if leaseTTL <= 0 {
		leaseTTL = action.DefaultIdempotencyLeaseTTL
	}

	claimKey := claimPrefix + key
	item, err := c.kv.Get(claimKey)
	if err != nil {
		return mapIdempotencyKVError(ctx, err)
	}
	activeClaim, err := decodeKVClaim(item.Value())
	if err != nil {
		return err
	}
	if activeClaim.Token != token {
		return xerr.Conflict("idempotency claim is owned by another token")
	}
	now := time.Now()
	if now.UnixNano() >= activeClaim.ExpiresAt {
		return xerr.Conflict("idempotency claim has expired")
	}
	expiresAt, err := idempotencyExpiry(now, leaseTTL)
	if err != nil {
		return err
	}
	activeClaim.ExpiresAt = expiresAt
	data, err := json.Marshal(activeClaim)
	if err != nil {
		return xerr.Internal("failed to marshal idempotency claim", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := c.kv.Update(claimKey, data, item.Revision()); err != nil {
		return mapIdempotencyKVError(ctx, err)
	}
	return nil
}

func EnsureIdempotencyBucket(js nats.JetStreamContext, name string, ttl time.Duration) error {
	if js == nil {
		return xerr.BadRequest("idempotency JetStream context is required")
	}
	if name == "" {
		return xerr.BadRequest("idempotency bucket name is required")
	}
	if ttl < 0 {
		return xerr.BadRequest("idempotency bucket TTL must not be negative")
	}
	_, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:      name,
		Description: "Nexss Distributed Idempotency Ledger",
		TTL:         ttl,
		Storage:     nats.FileStorage,
	})
	if err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		return MapError(fmt.Errorf("tnats: failed to ensure idempotency bucket: %w", err))
	}
	return nil
}
