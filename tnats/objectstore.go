package tnats

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
)

type ObjectEventType uint8

const (
	ObjectPut ObjectEventType = iota
	ObjectDelete
)

func (e ObjectEventType) String() string {
	switch e {
	case ObjectPut:
		return "PUT"
	case ObjectDelete:
		return "DELETE"
	}

	return "UNKNOWN"
}

type ObjectEvent struct {
	Bucket    string          `json:"bucket"`
	Name      string          `json:"name"`
	Type      ObjectEventType `json:"type"`
	Size      uint64          `json:"size,omitempty"`
	Chunks    uint32          `json:"chunks,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
	Data      []byte          `json:"data,omitempty"`
}

type ObjectBinding struct {
	Bucket      string
	Pattern     string
	IncludeData bool
	MaxBytes    int64
}

func (b ObjectBinding) String() string {
	return "nats objectstore: " + b.Bucket + "/" + b.Pattern
}

func ObjectStore(bucket, pattern string) ObjectBinding {
	return ObjectBinding{Bucket: bucket, Pattern: pattern, MaxBytes: 8 * 1024 * 1024}
}

func (b ObjectBinding) WithData(maxBytes int64) ObjectBinding {
	b.IncludeData = true
	if maxBytes > 0 {
		b.MaxBytes = maxBytes
	}

	return b
}

func validateObjectBinding(b ObjectBinding) error {
	if b.Bucket == "" {
		return xerr.BadRequest("objectstore requires a bucket name")
	}

	return nil
}

func (t *Transport) mountObjectStore(
	ctx context.Context, js nats.JetStreamContext, ex action.Executable, b ObjectBinding,
) error {
	if err := validateObjectBinding(b); err != nil {
		return err
	}

	obs, err := js.ObjectStore(b.Bucket)
	if err != nil {
		return MapError(err)
	}
	watcher, err := obs.Watch()
	if err != nil {
		return MapError(err)
	}

	pattern := b.Pattern
	if pattern == "" {
		pattern = ">"
	}

	t.workersWg.Go(func() {
		t.watchObjectStore(ctx, obs, watcher, ex, b, pattern)
	})

	return nil
}

func (t *Transport) watchObjectStore(
	ctx context.Context, obs nats.ObjectStore, watcher nats.ObjectWatcher,
	ex action.Executable, b ObjectBinding, pattern string,
) {
	backoff := 100 * time.Millisecond
	for {
		t.consumeObjectEvents(ctx, obs, ex, b, pattern, watcher)
		if ctx.Err() != nil {
			return
		}

		for {
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}

			nextWatcher, err := obs.Watch()
			if err != nil {
				t.log.Error("object_watch_failed", "bucket", b.Bucket, "error", MapError(err), "retry_delay", backoff)
				if backoff < 5*time.Second {
					backoff *= 2
					if backoff > 5*time.Second {
						backoff = 5 * time.Second
					}
				}
				continue
			}

			watcher = nextWatcher
			backoff = 100 * time.Millisecond
			break
		}
	}
}

func (t *Transport) consumeObjectEvents(
	ctx context.Context, obs nats.ObjectStore, ex action.Executable,
	b ObjectBinding, pattern string, w nats.ObjectWatcher,
) {
	stopDone := make(chan struct{})
	defer close(stopDone)
	var stopOnce sync.Once
	stopWatcher := func() {
		stopOnce.Do(func() {
			if err := w.Stop(); err != nil {
				t.log.Warn("object_watch_stop_failed", "bucket", b.Bucket, "error", MapError(err))
			}
		})
	}

	go func() {
		select {
		case <-ctx.Done():
			stopWatcher()
		case <-stopDone:
		}
	}()

	for info := range w.Updates() {
		if ctx.Err() != nil {
			break
		}

		if info == nil || !objectNameMatches(info.Name, pattern) {
			continue
		}

		ev := objectEventFromInfo(b, info)
		if b.IncludeData && ev.Type == ObjectPut {
			if data, derr := readObjectBytes(ctx, obs, info.Name, b.MaxBytes); derr == nil {
				ev.Data = data
			} else {
				t.log.Warn("object_fetch_failed", "name", info.Name, "error", derr)
			}
		}

		reqCtx, scope, release := xctx.NewScope(ctx)
		scope.Endpoint = "nats.object." + b.Bucket + "." + info.Name

		decoder := func(v any) error { return t.coerceObjectEvent(ev, v) }
		if _, execErr := ex.ExecuteDecoded(reqCtx, decoder); execErr != nil {
			t.log.Error("object_action_failed", "name", info.Name, "error", execErr)
		}

		release()
	}

	stopWatcher()
}

func objectEventFromInfo(b ObjectBinding, info *nats.ObjectInfo) ObjectEvent {
	ev := ObjectEvent{
		Bucket:    b.Bucket,
		Name:      info.Name,
		Size:      info.Size,
		Chunks:    info.Chunks,
		Timestamp: info.ModTime,
	}
	if info.Deleted {
		ev.Type = ObjectDelete
	} else {
		ev.Type = ObjectPut
	}

	return ev
}

// objectNameMatches performs NATS wildcard matching ('*', '>') with zero heap
// allocations. Uses index arithmetic instead of strings.Split.
func objectNameMatches(name, pattern string) bool {
	if pattern == "" || pattern == ">" {
		return true
	}

	nIdx, pIdx := 0, 0
	nLen, pLen := len(name), len(pattern)

	for pIdx < pLen {
		pEnd := pIdx
		for pEnd < pLen && pattern[pEnd] != '.' {
			pEnd++
		}

		pToken := pattern[pIdx:pEnd]

		if pToken == ">" {
			return nIdx < nLen
		}

		if nIdx >= nLen {
			return false
		}

		nEnd := nIdx
		for nEnd < nLen && name[nEnd] != '.' {
			nEnd++
		}

		nToken := name[nIdx:nEnd]

		if pToken != "*" && pToken != nToken {
			return false
		}

		if pEnd < pLen {
			pIdx = pEnd + 1
		} else {
			pIdx = pLen
		}

		if nEnd < nLen {
			nIdx = nEnd + 1
		} else {
			nIdx = nLen
		}
	}

	return nIdx == nLen && pIdx == pLen
}

func readObjectBytes(ctx context.Context, obs nats.ObjectStore, name string, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		maxBytes = 8 * 1024 * 1024
	}
	if maxBytes == math.MaxInt64 {
		return nil, xerr.BadRequest("object read limit is too large")
	}
	obj, err := obs.Get(name, nats.Context(ctx))
	if err != nil {
		return nil, MapError(err)
	}

	var buf bytes.Buffer
	_, readErr := io.CopyN(&buf, obj, maxBytes+1)
	closeErr := obj.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return nil, MapError(errors.Join(readErr, closeErr))
	}
	if closeErr != nil {
		return nil, MapError(closeErr)
	}

	if int64(buf.Len()) > maxBytes {
		return nil, xerr.BadRequest(fmt.Sprintf("object %q exceeds limit of %d bytes", name, maxBytes))
	}

	return buf.Bytes(), nil
}

func (t *Transport) coerceObjectEvent(ev ObjectEvent, v any) error {
	if p, ok := v.(*ObjectEvent); ok {
		*p = ev

		return nil
	}

	data, err := t.codec.Marshal(ev)
	if err != nil {
		return err
	}

	return t.codec.Unmarshal(data, v)
}

func (t *Transport) PutObject(ctx context.Context, bucket, name string, data []byte) (*nats.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if bucket == "" || name == "" {
		return nil, xerr.BadRequest("objectstore requires a bucket and object name")
	}
	if _, err := t.EnsureConn(ctx); err != nil {
		return nil, err
	}

	js, err := t.ensureJetStream()
	if err != nil {
		return nil, err
	}

	obs, err := js.ObjectStore(bucket)
	if err != nil {
		return nil, MapError(err)
	}

	info, err := obs.PutBytes(name, data, nats.Context(ctx))
	if err != nil {
		return nil, MapError(err)
	}

	return info, nil
}

func (t *Transport) GetObject(ctx context.Context, bucket, name string) ([]byte, error) {
	return t.GetObjectLimited(ctx, bucket, name, 8*1024*1024)
}

// GetObjectLimited reads at most maxBytes+1 bytes to detect oversized objects
// without buffering an unbounded object in memory.
func (t *Transport) GetObjectLimited(ctx context.Context, bucket, name string, maxBytes int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if bucket == "" || name == "" {
		return nil, xerr.BadRequest("objectstore requires a bucket and object name")
	}
	if maxBytes <= 0 {
		maxBytes = 8 * 1024 * 1024
	}
	if maxBytes == math.MaxInt64 {
		return nil, xerr.BadRequest("object read limit is too large")
	}
	if _, err := t.EnsureConn(ctx); err != nil {
		return nil, err
	}

	js, err := t.ensureJetStream()
	if err != nil {
		return nil, err
	}

	obs, err := js.ObjectStore(bucket)
	if err != nil {
		return nil, MapError(err)
	}

	return readObjectBytes(ctx, obs, name, maxBytes)
}

func (t *Transport) DeleteObject(ctx context.Context, bucket, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if bucket == "" || name == "" {
		return xerr.BadRequest("objectstore requires a bucket and object name")
	}

	nc, err := t.EnsureConn(ctx)
	if err != nil {
		return err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return MapError(err)
	}
	obs, err := js.ObjectStore(ctx, bucket)
	if err != nil {
		return MapError(err)
	}

	return MapError(obs.Delete(ctx, name))
}

var _ action.Binding = ObjectBinding{}
