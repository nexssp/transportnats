package tnats

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
)

func (t *Transport) mountKV(ctx context.Context, js nats.JetStreamContext, ex action.Executable, b KVBinding) error {
	kv, err := js.KeyValue(b.Bucket)
	if err != nil {
		return MapError(err)
	}

	t.workersWg.Go(func() {
		t.watchKV(ctx, kv, ex, b)
	})

	return nil
}

func (t *Transport) watchKV(ctx context.Context, kv nats.KeyValue, ex action.Executable, b KVBinding) {
	backoff := 100 * time.Millisecond

	for {
		if ctx.Err() != nil {
			return
		}

		w, err := kv.Watch(b.Key, nats.IgnoreDeletes())
		if err != nil {
			t.log.Error("kv_watch_failed", "bucket", b.Bucket, "key", b.Key, "error", err)

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
		t.consumeKVUpdates(ctx, w, ex, b)

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
	}
}

func (t *Transport) consumeKVUpdates(ctx context.Context, w nats.KeyWatcher, ex action.Executable, b KVBinding) {
	stopDone := make(chan struct{})
	defer close(stopDone)

	go func() {
		select {
		case <-ctx.Done():
			_ = w.Stop() //nolint:errcheck // watcher stop is best-effort
		case <-stopDone:
		}
	}()

	for entry := range w.Updates() {
		if ctx.Err() != nil {
			break
		}

		if entry == nil {
			continue
		}

		reqCtx, scope, release := xctx.NewScope(ctx)
		scope.Endpoint = "nats.kv." + b.Bucket + "." + b.Key

		decoder := func(v any) error { return t.codec.Unmarshal(entry.Value(), v) }
		if _, execErr := ex.ExecuteDecoded(reqCtx, decoder); execErr != nil {
			t.log.Error("kv_action_exec_failed", "bucket", b.Bucket, "key", b.Key, "error", execErr)
		}

		release()
	}

	_ = w.Stop() //nolint:errcheck // watcher stop is best-effort
}
