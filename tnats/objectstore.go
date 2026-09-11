package tnats

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/nats-io/nats.go"
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

	pattern := b.Pattern
	if pattern == "" {
		pattern = ">"
	}

	t.workersWg.Add(1)
	go func() {
		defer t.workersWg.Done()

		backoff := 100 * time.Millisecond

		for {
			if ctx.Err() != nil {
				return
			}

			w, wErr := obs.Watch()
			if wErr != nil {
				t.log.Error("object_watch_failed", "bucket", b.Bucket, "error", wErr)

				select {
				case <-time.After(backoff):
					if backoff < 5*time.Second {
						backoff *= 2
					}

					continue
				case <-ctx.Done():
					return
				}
			}

			backoff = 100 * time.Millisecond

			stopDone := make(chan struct{})

			go func() {
				select {
				case <-ctx.Done():
					_ = w.Stop()
				case <-stopDone:
				}
			}()

			for info := range w.Updates() {
				if ctx.Err() != nil {
					break
				}

				if info == nil {
					continue
				}

				if !objectNameMatches(info.Name, pattern) {
					continue
				}

				ev := objectEventFromInfo(b, info)
				if b.IncludeData && ev.Type == ObjectPut {
					if data, derr := readObjectBytes(obs, info.Name, b.MaxBytes); derr == nil {
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

			close(stopDone)

			_ = w.Stop()

			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
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

// objectNameMatches realizuje dopasowanie masek NATS ('*', '>') bez alokacji sterty (Zero Heap Allocs).
// Wykorzystuje arytmetykę indeksów bez wywoływania strings.Split().
func objectNameMatches(name, pattern string) bool {
	if pattern == "" || pattern == ">" {
		return true
	}

	nIdx, pIdx := 0, 0
	nLen, pLen := len(name), len(pattern)

	for pIdx < pLen {
		// Wyznaczenie końca tokenu we wzorcu
		pEnd := pIdx
		for pEnd < pLen && pattern[pEnd] != '.' {
			pEnd++
		}

		pToken := pattern[pIdx:pEnd]

		// NATS wildcard '>' dopasowuje wszystko do końca
		if pToken == ">" {
			return nIdx < nLen
		}

		if nIdx >= nLen {
			return false
		}

		// Wyznaczenie końca tokenu w nazwie
		nEnd := nIdx
		for nEnd < nLen && name[nEnd] != '.' {
			nEnd++
		}

		nToken := name[nIdx:nEnd]

		// NATS wildcard '*' dopasowuje dokładnie jeden token
		if pToken != "*" && pToken != nToken {
			return false
		}

		// Przejście do następnego segmentu
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

func readObjectBytes(obs nats.ObjectStore, name string, maxBytes int64) ([]byte, error) {
	obj, err := obs.Get(name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = obj.Close() }()

	if maxBytes <= 0 {
		maxBytes = 8 * 1024 * 1024
	}

	var buf bytes.Buffer
	if _, err := io.CopyN(&buf, obj, maxBytes+1); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
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

	js, err := t.ensureJetStream()
	if err != nil {
		return nil, err
	}

	obs, err := js.ObjectStore(bucket)
	if err != nil {
		return nil, MapError(err)
	}

	info, err := obs.PutBytes(name, data)
	if err != nil {
		return nil, MapError(err)
	}

	return info, nil
}

func (t *Transport) GetObject(ctx context.Context, bucket, name string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
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

	return readObjectBytes(obs, name, 0)
}

func (t *Transport) DeleteObject(ctx context.Context, bucket, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	js, err := t.ensureJetStream()
	if err != nil {
		return err
	}

	obs, err := js.ObjectStore(bucket)
	if err != nil {
		return MapError(err)
	}

	return MapError(obs.Delete(name))
}

var _ action.Binding = ObjectBinding{}
