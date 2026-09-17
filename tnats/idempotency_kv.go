package tnats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
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

type kvClaimPayload struct {
	Token       string `json:"token"`
	RequestHash string `json:"req_hash"`
	ExpiresAt   int64  `json:"exp"`
}

type kvEntryPayload struct {
	Entry     action.IdempotencyEntry `json:"entry"`
	ExpiresAt int64                   `json:"exp"`
}

var claimSequence atomic.Uint64

// KVIdempotencyCoordinator implements action.IdempotencyCoordinator backed by NATS JetStream KV.
type KVIdempotencyCoordinator struct {
	kv nats.KeyValue
}

var _ action.IdempotencyCoordinator = (*KVIdempotencyCoordinator)(nil)

// NewKVIdempotencyCoordinator creates a distributed idempotency coordinator backed by JetStream KV.
func NewKVIdempotencyCoordinator(js nats.JetStreamContext, bucketName ...string) (*KVIdempotencyCoordinator, error) {
	name := defaultIdempotencyBucket
	if len(bucketName) > 0 && bucketName[0] != "" {
		name = bucketName[0]
	}

	kv, err := js.KeyValue(name)
	if errors.Is(err, nats.ErrBucketNotFound) {
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:      name,
			Description: "Nexss Distributed Idempotency Ledger",
			TTL:         24 * time.Hour,
			History:     1,
			Storage:     nats.FileStorage,
		})
	}

	if err != nil {
		return nil, MapError(err)
	}

	return &KVIdempotencyCoordinator{kv: kv}, nil
}

// Get retrieves a stored response for a completed request.
func (c *KVIdempotencyCoordinator) Get(_ context.Context, key string) (action.IdempotencyEntry, bool) {
	entryKey := entryPrefix + key

	item, err := c.kv.Get(entryKey)
	if err != nil {
		return action.IdempotencyEntry{}, false
	}

	var stored kvEntryPayload
	if jsonErr := json.Unmarshal(item.Value(), &stored); jsonErr == nil && stored.ExpiresAt != 0 {
		if time.Now().UnixNano() >= stored.ExpiresAt {
			return action.IdempotencyEntry{}, false
		}

		return stored.Entry, true
	}

	// Accept entries written by older versions that stored the entry directly.
	var entry action.IdempotencyEntry
	if jsonErr := json.Unmarshal(item.Value(), &entry); jsonErr != nil {
		return action.IdempotencyEntry{}, false
	}

	return entry, true
}

// Set stores the result in the backend.
func (c *KVIdempotencyCoordinator) Set(
	_ context.Context, key string, entry action.IdempotencyEntry, ttl time.Duration,
) {
	entryKey := entryPrefix + key

	data, err := marshalKVEntry(entry, ttl)
	if err != nil {
		return
	}

	_, _ = c.kv.Put(entryKey, data) //nolint:errcheck // KV put failure surfaces on the next Get
}

// Claim atomically reserves an idempotency token in NATS KV.
func (c *KVIdempotencyCoordinator) Claim(
	ctx context.Context, key, requestHash string, leaseTTL time.Duration,
) (action.IdempotencyClaim, error) {
	if err := ctx.Err(); err != nil {
		return action.IdempotencyClaim{}, err
	}

	if entry, ok := c.Get(ctx, key); ok {
		if entry.RequestHash != "" && entry.RequestHash != requestHash {
			return action.IdempotencyClaim{State: action.IdempotencyClaimConflict}, nil
		}

		return action.IdempotencyClaim{
			State: action.IdempotencyClaimCompleted,
			Entry: entry,
		}, nil
	}

	claimKey := claimPrefix + key
	now := time.Now().UnixNano()
	token := strconv.FormatInt(now, 10) + "-" + strconv.FormatUint(claimSequence.Add(1), 10)

	payload := kvClaimPayload{
		Token:       token,
		RequestHash: requestHash,
		ExpiresAt:   now + leaseTTL.Nanoseconds(),
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return action.IdempotencyClaim{}, xerr.Internal("failed to marshal claim", err)
	}

	// Create succeeds atomically only when the key does not yet exist.
	_, createErr := c.kv.Create(claimKey, data)
	if createErr == nil {
		return action.IdempotencyClaim{
			State: action.IdempotencyClaimAcquired,
			Token: token,
		}, nil
	}

	existing, getErr := c.kv.Get(claimKey)
	if getErr != nil {
		if errors.Is(getErr, nats.ErrKeyNotFound) {
			return action.IdempotencyClaim{State: action.IdempotencyClaimInProgress}, nil
		}

		return action.IdempotencyClaim{}, MapError(getErr)
	}

	var activeClaim kvClaimPayload
	if err := json.Unmarshal(existing.Value(), &activeClaim); err != nil {
		return action.IdempotencyClaim{}, xerr.Internal("corrupted idempotency claim in KV", err)
	}

	if activeClaim.RequestHash != "" && activeClaim.RequestHash != requestHash {
		return action.IdempotencyClaim{State: action.IdempotencyClaimConflict}, nil
	}

	if now > activeClaim.ExpiresAt {
		_, updateErr := c.kv.Update(claimKey, data, existing.Revision())
		if updateErr == nil {
			return action.IdempotencyClaim{
				State: action.IdempotencyClaimAcquired,
				Token: token,
			}, nil
		}
	}

	return action.IdempotencyClaim{State: action.IdempotencyClaimInProgress}, nil
}

// Complete writes the result before releasing the claim, then removes only the
// exact claim revision owned by token.
func (c *KVIdempotencyCoordinator) Complete(
	ctx context.Context, key, token string, entry action.IdempotencyEntry, ttl time.Duration,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	claimKey := claimPrefix + key

	item, err := c.kv.Get(claimKey)
	if err != nil {
		return MapError(err)
	}

	var activeClaim kvClaimPayload
	if jsonErr := json.Unmarshal(item.Value(), &activeClaim); jsonErr != nil {
		return xerr.Internal("corrupted idempotency claim in KV", jsonErr)
	}

	if activeClaim.Token != token {
		return xerr.Conflict("idempotency claim is owned by another token")
	}

	data, err := marshalKVEntry(entry, ttl)
	if err != nil {
		return xerr.Internal("failed to marshal idempotency entry", err)
	}

	if _, err = c.kv.Put(entryPrefix+key, data); err != nil {
		return MapError(err)
	}

	if err := c.kv.Delete(claimKey, nats.LastRevision(item.Revision())); err != nil {
		return MapError(err)
	}

	return nil
}

// Release frees the reservation after a failure or panic. Release is
// best-effort: corrupted claim data cannot be safely deleted by token, so it
// is left in place and expires naturally through the lease TTL.
func (c *KVIdempotencyCoordinator) Release(_ context.Context, key, token string) error {
	claimKey := claimPrefix + key

	item, err := c.kv.Get(claimKey)
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			return nil
		}

		return MapError(err)
	}

	var activeClaim kvClaimPayload
	if jsonErr := json.Unmarshal(item.Value(), &activeClaim); jsonErr != nil {
		return nil //nolint:nilerr // corrupted claim data cannot be safely released; lease expires naturally
	}

	if activeClaim.Token == token {
		return MapError(c.kv.Delete(claimKey, nats.LastRevision(item.Revision())))
	}

	return nil
}

func marshalKVEntry(entry action.IdempotencyEntry, ttl time.Duration) ([]byte, error) {
	expiresAt := int64(0)
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl).UnixNano()
	}

	return json.Marshal(kvEntryPayload{Entry: entry, ExpiresAt: expiresAt})
}

func EnsureIdempotencyBucket(js nats.JetStreamContext, name string, ttl time.Duration) error {
	_, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:      name,
		Description: "Nexss Distributed Idempotency Ledger",
		TTL:         ttl,
		Storage:     nats.FileStorage,
	})
	if err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		return fmt.Errorf("tnats: failed to ensure idempotency bucket: %w", err)
	}

	return nil
}
