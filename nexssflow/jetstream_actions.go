package nexssflow

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"

	"github.com/nexssp/transportnats/tnats"
)

const defaultFlowObjectMaxBytes int64 = 8 * 1024 * 1024

// KVGetRequest selects one key in an existing JetStream KV bucket.
type KVGetRequest struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

// KVGetResponse returns JSON values as decoded Flow values and other bytes as base64.
type KVGetResponse struct {
	Bucket   string    `json:"bucket"`
	Key      string    `json:"key"`
	Found    bool      `json:"found"`
	Value    any       `json:"value"`
	Encoding string    `json:"encoding,omitempty"`
	Revision uint64    `json:"revision,omitempty"`
	Created  time.Time `json:"created,omitzero"`
}

// KVPutRequest writes a JSON-encoded value unconditionally.
type KVPutRequest struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	Value  any    `json:"value"`
}

// KVWriteResponse identifies the committed JetStream revision.
type KVWriteResponse struct {
	Bucket   string `json:"bucket"`
	Key      string `json:"key"`
	Revision uint64 `json:"revision"`
}

// KVCreateRequest creates a key only if it does not exist. TTL is a Go duration string.
type KVCreateRequest struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	Value  any    `json:"value"`
	TTL    string `json:"ttl,omitempty"`
}

// KVUpdateRequest performs an optimistic-concurrency update.
type KVUpdateRequest struct {
	Bucket   string `json:"bucket"`
	Key      string `json:"key"`
	Value    any    `json:"value"`
	Revision uint64 `json:"revision"`
}

// KVDeleteRequest deletes a key; Revision, when supplied, enables CAS protection.
type KVDeleteRequest struct {
	Bucket   string  `json:"bucket"`
	Key      string  `json:"key"`
	Revision *uint64 `json:"revision,omitempty"`
}

// ObjectPutRequest stores UTF-8 text by default or base64-encoded bytes when encoding is "base64".
type ObjectPutRequest struct {
	Bucket   string `json:"bucket"`
	Name     string `json:"name"`
	Data     string `json:"data"`
	Encoding string `json:"encoding,omitempty"`
	MaxBytes int64  `json:"max_bytes,omitempty"`
}

// ObjectGetRequest retrieves an object, stopping once its configured byte limit is exceeded.
type ObjectGetRequest struct {
	Bucket   string `json:"bucket"`
	Name     string `json:"name"`
	MaxBytes int64  `json:"max_bytes,omitempty"`
}

// ObjectGetResponse returns UTF-8 data directly and arbitrary binary data as base64.
type ObjectGetResponse struct {
	Bucket   string `json:"bucket"`
	Name     string `json:"name"`
	Data     string `json:"data"`
	Encoding string `json:"encoding"`
	Size     int64  `json:"size"`
}

// ObjectDeleteRequest deletes a named object from an existing bucket.
type ObjectDeleteRequest struct {
	Bucket string `json:"bucket"`
	Name   string `json:"name"`
}

func jetStreamActions(tr *tnats.Transport, cfg Config) []action.AnyAction {
	return []action.AnyAction{
		action.New("nats.kv.get", func(ctx context.Context, req KVGetRequest) (KVGetResponse, error) {
			if err := validateKVRequest("nats.kv.get", req.Bucket, req.Key); err != nil {
				return KVGetResponse{}, err
			}
			callCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
			defer cancel()

			entry, err := tr.GetKV(callCtx, req.Bucket, req.Key)
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				return KVGetResponse{Bucket: req.Bucket, Key: req.Key, Found: false}, nil
			}
			if err != nil {
				return KVGetResponse{}, err
			}
			value, encoding := decodeKVValue(entry.Value)
			return KVGetResponse{
				Bucket: req.Bucket, Key: req.Key, Found: true, Value: value, Encoding: encoding,
				Revision: entry.Revision, Created: entry.Created,
			}, nil
		}).Description("Read a JetStream KV value with its revision").Tag("transport", "nats", "kv", "get").Build(),

		action.New("nats.kv.put", func(ctx context.Context, req KVPutRequest) (KVWriteResponse, error) {
			if err := validateKVRequest("nats.kv.put", req.Bucket, req.Key); err != nil {
				return KVWriteResponse{}, err
			}
			data, err := marshalKVValue(req.Value)
			if err != nil {
				return KVWriteResponse{}, xerr.BadRequest("nats.kv.put: value must be JSON encodable", err)
			}
			callCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
			defer cancel()
			revision, err := tr.PutKV(callCtx, req.Bucket, req.Key, data)
			if err != nil {
				return KVWriteResponse{}, err
			}
			return KVWriteResponse{Bucket: req.Bucket, Key: req.Key, Revision: revision}, nil
		}).Description("Write a JSON value to JetStream KV without a revision precondition").Tag("transport", "nats", "kv", "put").Build(),

		action.New("nats.kv.create", func(ctx context.Context, req KVCreateRequest) (KVWriteResponse, error) {
			if err := validateKVRequest("nats.kv.create", req.Bucket, req.Key); err != nil {
				return KVWriteResponse{}, err
			}
			data, err := marshalKVValue(req.Value)
			if err != nil {
				return KVWriteResponse{}, xerr.BadRequest("nats.kv.create: value must be JSON encodable", err)
			}
			ttl, err := parseKVTTL(req.TTL)
			if err != nil {
				return KVWriteResponse{}, xerr.BadRequest("nats.kv.create: ttl must be at least 1s, for example 15m", err)
			}
			callCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
			defer cancel()
			revision, err := tr.CreateKV(callCtx, req.Bucket, req.Key, data, ttl)
			if err != nil {
				return KVWriteResponse{}, err
			}
			return KVWriteResponse{Bucket: req.Bucket, Key: req.Key, Revision: revision}, nil
		}).Description("Create a JetStream KV value only when absent; optionally assign a per-key TTL").Tag("transport", "nats", "kv", "create").Build(),

		action.New("nats.kv.update", func(ctx context.Context, req KVUpdateRequest) (KVWriteResponse, error) {
			if err := validateKVRequest("nats.kv.update", req.Bucket, req.Key); err != nil {
				return KVWriteResponse{}, err
			}
			if req.Revision == 0 {
				return KVWriteResponse{}, xerr.BadRequest("nats.kv.update: revision is required and must be greater than zero")
			}
			data, err := marshalKVValue(req.Value)
			if err != nil {
				return KVWriteResponse{}, xerr.BadRequest("nats.kv.update: value must be JSON encodable", err)
			}
			callCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
			defer cancel()
			revision, err := tr.UpdateKV(callCtx, req.Bucket, req.Key, data, req.Revision)
			if err != nil {
				return KVWriteResponse{}, err
			}
			return KVWriteResponse{Bucket: req.Bucket, Key: req.Key, Revision: revision}, nil
		}).Description("Compare-and-set a JetStream KV value using its current revision").Tag("transport", "nats", "kv", "update").Build(),

		action.New("nats.kv.delete", func(ctx context.Context, req KVDeleteRequest) (map[string]any, error) {
			if err := validateKVRequest("nats.kv.delete", req.Bucket, req.Key); err != nil {
				return nil, err
			}
			if req.Revision != nil && *req.Revision == 0 {
				return nil, xerr.BadRequest("nats.kv.delete: revision must be greater than zero")
			}
			callCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
			defer cancel()
			if err := tr.DeleteKV(callCtx, req.Bucket, req.Key, req.Revision); err != nil {
				return nil, err
			}
			return map[string]any{"bucket": req.Bucket, "key": req.Key, "deleted": true}, nil
		}).Description("Delete a JetStream KV key, optionally guarded by its current revision").Tag("transport", "nats", "kv", "delete").Build(),

		objectPutAction(tr, cfg),
		objectGetAction(tr, cfg),
		objectDeleteAction(tr, cfg),
	}
}

func objectPutAction(tr *tnats.Transport, cfg Config) action.AnyAction {
	return action.New("nats.object.put", func(ctx context.Context, req ObjectPutRequest) (map[string]any, error) {
		if req.Bucket == "" || req.Name == "" {
			return nil, xerr.BadRequest("nats.object.put: bucket and name are required")
		}
		limit, err := objectLimit(req.MaxBytes)
		if err != nil {
			return nil, xerr.BadRequest("nats.object.put: max_bytes must be zero or positive", err)
		}
		data, err := decodeObjectInput(req.Data, req.Encoding)
		if err != nil {
			return nil, xerr.BadRequest("nats.object.put: invalid data or encoding", err)
		}
		if int64(len(data)) > limit {
			return nil, xerr.BadRequest(fmt.Sprintf("nats.object.put: data is %d bytes; limit is %d", len(data), limit))
		}
		callCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
		info, err := tr.PutObject(callCtx, req.Bucket, req.Name, data)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"bucket": req.Bucket, "name": req.Name, "size": info.Size,
			"chunks": info.Chunks, "digest": info.Digest,
		}, nil
	}).Description("Store a bounded text or base64 binary object in JetStream ObjectStore").Tag("transport", "nats", "object", "put").Build()
}

func objectGetAction(tr *tnats.Transport, cfg Config) action.AnyAction {
	return action.New("nats.object.get", func(ctx context.Context, req ObjectGetRequest) (ObjectGetResponse, error) {
		if req.Bucket == "" || req.Name == "" {
			return ObjectGetResponse{}, xerr.BadRequest("nats.object.get: bucket and name are required")
		}
		limit, err := objectLimit(req.MaxBytes)
		if err != nil {
			return ObjectGetResponse{}, xerr.BadRequest("nats.object.get: max_bytes must be zero or positive", err)
		}
		callCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
		data, err := tr.GetObjectLimited(callCtx, req.Bucket, req.Name, limit)
		if err != nil {
			return ObjectGetResponse{}, err
		}
		encoding, value := "utf-8", string(data)
		if !utf8.Valid(data) {
			encoding, value = "base64", base64.StdEncoding.EncodeToString(data)
		}
		return ObjectGetResponse{Bucket: req.Bucket, Name: req.Name, Data: value, Encoding: encoding, Size: int64(len(data))}, nil
	}).Description("Read a JetStream ObjectStore object without exceeding max_bytes").Tag("transport", "nats", "object", "get").Build()
}

func objectDeleteAction(tr *tnats.Transport, cfg Config) action.AnyAction {
	return action.New("nats.object.delete", func(ctx context.Context, req ObjectDeleteRequest) (map[string]any, error) {
		if req.Bucket == "" || req.Name == "" {
			return nil, xerr.BadRequest("nats.object.delete: bucket and name are required")
		}
		callCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
		if err := tr.DeleteObject(callCtx, req.Bucket, req.Name); err != nil {
			return nil, err
		}
		return map[string]any{"bucket": req.Bucket, "name": req.Name, "deleted": true}, nil
	}).Description("Delete a named object from JetStream ObjectStore").Tag("transport", "nats", "object", "delete").Build()
}

func validateKVRequest(actionName, bucket, key string) error {
	if strings.TrimSpace(bucket) == "" || strings.TrimSpace(key) == "" {
		return xerr.BadRequest(actionName + ": bucket and key are required")
	}
	return nil
}

func marshalKVValue(value any) ([]byte, error) {
	if value == nil {
		return nil, errors.New("value is required")
	}
	return json.Marshal(value)
}

func decodeKVValue(data []byte) (value any, encoding string) {
	var decoded any
	if json.Unmarshal(data, &decoded) == nil {
		return decoded, "json"
	}
	return base64.StdEncoding.EncodeToString(data), "base64"
}

func parseKVTTL(raw string) (time.Duration, error) {
	if raw == "" {
		return 0, nil
	}
	ttl, err := time.ParseDuration(raw)
	if err != nil {
		return 0, err
	}
	if ttl < time.Second {
		return 0, errors.New("ttl must be at least one second")
	}
	return ttl, nil
}

func objectLimit(maxBytes int64) (int64, error) {
	if maxBytes < 0 {
		return 0, errors.New("negative byte limit")
	}
	if maxBytes == 0 {
		return defaultFlowObjectMaxBytes, nil
	}
	return maxBytes, nil
}

func decodeObjectInput(data, encoding string) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "utf-8", "utf8":
		return []byte(data), nil
	case "base64":
		return base64.StdEncoding.DecodeString(data)
	default:
		return nil, fmt.Errorf("unsupported encoding %q; use utf-8 or base64", encoding)
	}
}
