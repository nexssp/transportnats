package tnats

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nexssp/kernel/xerr"
)

// KVEntry is a snapshot of one JetStream Key-Value value.
type KVEntry struct {
	Bucket   string
	Key      string
	Value    []byte
	Revision uint64
	Created  time.Time
}

func (t *Transport) keyValue(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	if bucket == "" {
		return nil, xerr.BadRequest("nats kv: bucket is required")
	}
	nc, err := t.EnsureConn(ctx)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, MapError(err)
	}
	kv, err := js.KeyValue(ctx, bucket)
	if err != nil {
		return nil, mapKVError(err, bucket, "")
	}
	return kv, nil
}

// GetKV reads the latest value for key from an existing bucket.
func (t *Transport) GetKV(ctx context.Context, bucket, key string) (KVEntry, error) {
	if key == "" {
		return KVEntry{}, xerr.BadRequest("nats kv: key is required")
	}
	kv, err := t.keyValue(ctx, bucket)
	if err != nil {
		return KVEntry{}, err
	}
	entry, err := kv.Get(ctx, key)
	if err != nil {
		return KVEntry{}, mapKVError(err, bucket, key)
	}
	return kvEntry(entry), nil
}

// PutKV unconditionally inserts or replaces a value. It does not alter per-key TTL.
func (t *Transport) PutKV(ctx context.Context, bucket, key string, value []byte) (uint64, error) {
	if key == "" {
		return 0, xerr.BadRequest("nats kv: key is required")
	}
	kv, err := t.keyValue(ctx, bucket)
	if err != nil {
		return 0, err
	}
	revision, err := kv.Put(ctx, key, value)
	if err != nil {
		return 0, mapKVError(err, bucket, key)
	}
	return revision, nil
}

// CreateKV creates a key only if it is absent. A positive TTL is applied to this
// entry on supported JetStream servers and buckets; TTL cannot be changed later.
func (t *Transport) CreateKV(ctx context.Context, bucket, key string, value []byte, ttl time.Duration) (uint64, error) {
	if key == "" {
		return 0, xerr.BadRequest("nats kv: key is required")
	}
	if ttl < 0 {
		return 0, xerr.BadRequest("nats kv: ttl must not be negative")
	}
	kv, err := t.keyValue(ctx, bucket)
	if err != nil {
		return 0, err
	}
	var opts []jetstream.KVCreateOpt
	if ttl > 0 {
		opts = append(opts, jetstream.KeyTTL(ttl))
	}
	revision, err := kv.Create(ctx, key, value, opts...)
	if err != nil {
		return 0, mapKVError(err, bucket, key)
	}
	return revision, nil
}

// UpdateKV performs a compare-and-set update against expectedRevision.
func (t *Transport) UpdateKV(ctx context.Context, bucket, key string, value []byte, expectedRevision uint64) (uint64, error) {
	if key == "" {
		return 0, xerr.BadRequest("nats kv: key is required")
	}
	if expectedRevision == 0 {
		return 0, xerr.BadRequest("nats kv: expected revision must be greater than zero")
	}
	kv, err := t.keyValue(ctx, bucket)
	if err != nil {
		return 0, err
	}
	revision, err := kv.Update(ctx, key, value, expectedRevision)
	if err != nil {
		return 0, mapKVError(err, bucket, key)
	}
	return revision, nil
}

// DeleteKV deletes a key, optionally only when expectedRevision still matches.
func (t *Transport) DeleteKV(ctx context.Context, bucket, key string, expectedRevision *uint64) error {
	if key == "" {
		return xerr.BadRequest("nats kv: key is required")
	}
	kv, err := t.keyValue(ctx, bucket)
	if err != nil {
		return err
	}
	var opts []jetstream.KVDeleteOpt
	if expectedRevision != nil {
		if *expectedRevision == 0 {
			return xerr.BadRequest("nats kv: expected revision must be greater than zero")
		}
		opts = append(opts, jetstream.LastRevision(*expectedRevision))
	}
	if err := kv.Delete(ctx, key, opts...); err != nil {
		return mapKVError(err, bucket, key)
	}
	return nil
}

func kvEntry(entry jetstream.KeyValueEntry) KVEntry {
	return KVEntry{
		Bucket:   entry.Bucket(),
		Key:      entry.Key(),
		Value:    append([]byte(nil), entry.Value()...),
		Revision: entry.Revision(),
		Created:  entry.Created(),
	}
}

func mapKVError(err error, bucket, key string) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, jetstream.ErrBucketNotFound):
		return xerr.NotFound(fmt.Sprintf("nats kv: bucket %q not found", bucket), err)
	case errors.Is(err, jetstream.ErrKeyNotFound):
		return xerr.NotFound(fmt.Sprintf("nats kv: key %q not found in bucket %q", key, bucket), err)
	case errors.Is(err, jetstream.ErrKeyExists):
		return xerr.Conflict(fmt.Sprintf("nats kv: key %q already exists in bucket %q", key, bucket), err)
	case errors.Is(err, jetstream.ErrKeyRevisionMismatch):
		return xerr.Conflict(fmt.Sprintf("nats kv: revision conflict for key %q in bucket %q", key, bucket), err)
	case errors.Is(err, jetstream.ErrInvalidKey), errors.Is(err, jetstream.ErrInvalidBucketName):
		return xerr.BadRequest("nats kv: invalid bucket or key", err)
	default:
		return MapError(err)
	}
}
