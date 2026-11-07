package tnats

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/transport"
)

// Do runs the listener loop for every mounted binding and blocks until
// the context is canceled or the connection is permanently closed.
//
// The connection is dialed lazily through EnsureConn. A transport that
// was constructed with a preexisting connection (FromConn) reuses it and
// never dials. A transport built from a URL dials on the first Do call.
//
// Do is safe to call once per transport. Concurrent calls return an
// error.
func (t *Transport) Do(ctx context.Context, _ any) (any, error) {
	if !t.running.CompareAndSwap(false, true) {
		return nil, errors.New("nats transport is already running")
	}
	defer t.running.Store(false)
	t.mu.Lock()
	t.closing = false
	t.mu.Unlock()

	t.mu.RLock()
	initErr := t.initErr
	t.mu.RUnlock()
	if initErr != nil {
		t.signalReady(initErr)
		return nil, initErr
	}

	if t.embedded != nil {
		if err := t.startEmbedded(); err != nil {
			t.signalReady(err)
			return nil, err
		}
		defer t.stopEmbedded()
	}

	runCtx, runCancel := context.WithCancel(ctx)

	nc, err := t.EnsureConn(runCtx)
	if err != nil {
		runCancel()
		t.signalReady(err)
		return nil, err
	}

	js, jsErr := t.ensureJetStream()
	if jsErr != nil {
		runCancel()
		t.signalReady(jsErr)
		return nil, jsErr
	}
	defer func() {
		runCancel()

		t.mu.Lock()
		t.closing = true
		services := t.microServices
		t.microServices = nil
		subs := t.subs
		t.subs = nil
		t.mu.Unlock()
		for _, svc := range services {
			if err := svc.Stop(); err != nil {
				t.log.Warn("nats_micro_service_stop_failed", "error", MapError(err))
			}
		}
		for _, sub := range subs {
			if err := sub.Unsubscribe(); err != nil {
				t.log.Warn("nats_subscription_unsubscribe_failed", "error", MapError(err))
			}
		}

		t.workersWg.Wait()
		if err := t.Close(); err != nil {
			t.log.Warn("nats_transport_close_failed", "error", MapError(err))
		}
	}()
	if err := t.configureIdempotencyStore(js); err != nil {
		t.signalReady(err)
		return nil, err
	}

	t.mu.RLock()
	actions := append([]action.AnyAction(nil), t.actions...)
	t.mu.RUnlock()

	if mountErr := t.mountActions(runCtx, nc, js, actions); mountErr != nil {
		t.signalReady(mountErr)
		return nil, mountErr
	}

	flushCtx, cancel := context.WithTimeout(runCtx, defaultShutdownTimeout)
	defer cancel()

	if flushErr := nc.FlushWithContext(flushCtx); flushErr != nil {
		mappedErr := MapError(flushErr)
		t.signalReady(mappedErr)
		return nil, mappedErr
	}

	t.signalReady(nil)
	t.log.Info("nats_transport_started", "url", sanitizeURL(nc.ConnectedUrl()))

	closedChan := nc.StatusChanged(nats.CLOSED)
	select {
	case <-runCtx.Done():
		t.log.Info("nats_transport_shutting_down")
		//nolint:nilnil // Context cancellation is a successful stop; the listener has no result value.
		return nil, nil
	case <-closedChan:
		if ctxErr := ctx.Err(); ctxErr != nil {
			//nolint:nilnil // Context cancellation is a successful stop; the listener has no result value.
			return nil, nil
		}
		return nil, xerr.Unavailable("nats: connection closed", nc.LastError())
	}
}

func (t *Transport) configureIdempotencyStore(js nats.JetStreamContext) error {
	if t.idempotencyBucket == "" {
		return nil
	}
	if t.idempotencyStoreSet {
		return xerr.Conflict("configure either an idempotency store or a JetStream KV bucket, not both")
	}
	if js == nil {
		return xerr.BadRequest("JetStream is required for the configured idempotency bucket")
	}
	if _, err := js.KeyValue(t.idempotencyBucket); err != nil {
		return xerr.Unavailable("open pre-provisioned shared idempotency bucket", MapError(err))
	}
	coordinator, err := NewKVIdempotencyCoordinator(js, t.idempotencyBucket)
	if err != nil {
		return err
	}
	t.idemStore = coordinator
	return nil
}

func (t *Transport) mountActions(
	ctx context.Context, nc *nats.Conn, js nats.JetStreamContext,
	actions []action.AnyAction,
) error {
	for _, act := range actions {
		ex, ok := act.(action.Executable)
		if !ok {
			continue
		}

		meta := act.Describe()

		for _, b := range act.GetBindings() {
			var err error

			switch binding := b.(type) {
			case TopicBinding:
				err = t.subscribeTopic(ctx, nc, ex, binding, meta, 0)
			case RequestBinding:
				err = t.subscribeTopic(ctx, nc, ex, TopicBinding{Subject: binding.Subject}, meta, binding.Timeout)
			case KVBinding:
				err = t.mountKV(ctx, js, ex, binding)
			case DurableBinding:
				err = t.subscribeDurable(ctx, nc, js, ex, binding, meta)
			case ConsumerBinding:
				err = t.subscribeConsumer(ctx, nc, js, ex, binding, meta)
			case ObjectBinding:
				err = t.mountObjectStore(ctx, js, ex, binding)
			case ServiceBinding:
				continue
			default:
				t.log.Warn("skipping_unsupported_binding", "binding", binding)
			}

			if err != nil {
				return err
			}
		}
	}

	return t.mountMicroServices(ctx, nc, actions)
}

func (t *Transport) subscribeTopic(
	ctx context.Context, nc *nats.Conn, ex action.Executable,
	b TopicBinding, meta *action.Meta, handlerTimeout time.Duration,
) error {
	handler := func(msg *nats.Msg) {
		if !t.startWorker() {
			return
		}
		go func(m *nats.Msg) {
			defer t.workersWg.Done()
			callCtx := ctx
			if handlerTimeout > 0 {
				var cancel context.CancelFunc
				callCtx, cancel = context.WithTimeout(ctx, handlerTimeout)
				defer cancel()
			}

			reqCtx, scope, release := xctx.NewScope(callCtx)
			defer release()

			scope.Endpoint = "nats." + m.Subject
			reqCtx = extractContextHeaders(reqCtx, m, scope)

			decoder := func(v any) error {
				if len(m.Data) == 0 {
					return nil
				}
				return t.codec.Unmarshal(m.Data, v)
			}

			var (
				replyBytes []byte
				err        error
			)

			if meta != nil && meta.Idempotency.Enabled {
				replyBytes, err = executeWithIdempotency(reqCtx, t.idemStore, meta.Idempotency, ex, m, decoder, t.codec)
			} else {
				res, execErr := ex.ExecuteDecoded(reqCtx, decoder)
				if execErr != nil {
					replyBytes, err = encodeError(t.codec, execErr)
				} else {
					replyBytes, err = t.codec.Marshal(res)
					if err != nil {
						replyBytes, err = encodeError(t.codec, xerr.Internal("failed to marshal response", err))
					}
				}
			}

			if m.Reply != "" {
				reply := nats.NewMsg(m.Reply)
				reply.Data = replyBytes
				if scope.RequestID != "" {
					reply.Header.Set(transport.HeaderRequestID, scope.RequestID)
				}
				if err != nil {
					reply.Header.Set(HeaderErrorMarker, "1")
				}
				if publishErr := nc.PublishMsg(reply); publishErr != nil {
					t.log.Error("nats_reply_publish_failed", "subject", m.Subject, "error", MapError(publishErr))
				}
			} else if err != nil {
				t.log.Error("nats_topic_action_failed", "subject", m.Subject, "error", err)
			}
		}(msg)
	}

	var (
		sub *nats.Subscription
		err error
	)

	if b.QueueGroup != "" {
		sub, err = nc.QueueSubscribe(b.Subject, b.QueueGroup, handler)
	} else {
		sub, err = nc.Subscribe(b.Subject, handler)
	}

	if err != nil {
		return MapError(err)
	}

	t.mu.Lock()
	t.subs = append(t.subs, sub)
	t.mu.Unlock()

	return nil
}
