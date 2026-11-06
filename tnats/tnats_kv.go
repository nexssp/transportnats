package tnats

import (
	"context"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
)

func (t *Transport) mountKV(ctx context.Context, js nats.JetStreamContext, ex action.Executable, b KVBinding) error {
	if b.Bucket == "" {
		return xerr.BadRequest("KV watcher requires a bucket name")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	kv, err := js.KeyValue(b.Bucket)
	if err != nil {
		return MapError(err)
	}
	watcher, err := openKVWatcher(kv, b.Key)
	if err != nil {
		return MapError(err)
	}

	t.workersWg.Go(func() {
		t.watchKV(ctx, kv, watcher, ex, b)
	})
	return nil
}

func openKVWatcher(kv nats.KeyValue, key string) (nats.KeyWatcher, error) {
	if key == "" {
		return kv.WatchAll(nats.IgnoreDeletes())
	}
	return kv.Watch(key, nats.IgnoreDeletes())
}

func (t *Transport) watchKV(ctx context.Context, kv nats.KeyValue, watcher nats.KeyWatcher, ex action.Executable, b KVBinding) {
	backoff := 100 * time.Millisecond
	for {
		t.consumeKVUpdates(ctx, watcher, ex, b)
		if ctx.Err() != nil {
			return
		}

		for {
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}

			nextWatcher, err := openKVWatcher(kv, b.Key)
			if err != nil {
				t.log.Error("kv_watch_failed", "bucket", b.Bucket, "key", b.Key, "error", MapError(err), "retry_delay", backoff)
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

func (t *Transport) consumeKVUpdates(ctx context.Context, watcher nats.KeyWatcher, ex action.Executable, b KVBinding) {
	stopDone := make(chan struct{})
	defer close(stopDone)
	var stopOnce sync.Once
	stopWatcher := func() {
		stopOnce.Do(func() {
			if err := watcher.Stop(); err != nil {
				t.log.Warn("kv_watch_stop_failed", "bucket", b.Bucket, "key", b.Key, "error", MapError(err))
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

	for entry := range watcher.Updates() {
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
			t.log.Error("kv_action_exec_failed", "bucket", b.Bucket, "key", entry.Key(), "error", execErr)
		}
		release()
	}
	stopWatcher()
}
