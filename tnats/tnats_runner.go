package tnats

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/transport"
)

func (t *Transport) Do(ctx context.Context, _ any) (any, error) {
	if !t.running.CompareAndSwap(false, true) {
		return nil, errors.New("nats transport is already running")
	}
	defer t.running.Store(false)

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

	t.mu.RLock()
	activeURL := t.url
	t.mu.RUnlock()

	if activeURL == "" {
		err := xerr.Internal("NATS URL is required")
		t.signalReady(err)

		return nil, err
	}

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	opts := append([]nats.Option{
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectBufSize(8 * 1024 * 1024),
		nats.Timeout(3 * time.Second),
	}, t.options...)

	nc, err := nats.Connect(activeURL, opts...)
	if err != nil {
		connErr := MapError(err)
		t.signalReady(connErr)

		return nil, connErr
	}

	js, jsErr := nc.JetStream()
	if jsErr != nil {
		nc.Close()

		mappedErr := MapError(jsErr)
		t.signalReady(mappedErr)

		return nil, mappedErr
	}

	t.mu.Lock()
	t.conn = nc
	t.js = js
	t.mu.Unlock()

	defer func() {
		runCancel()

		t.mu.Lock()
		for _, svc := range t.microServices {
			_ = svc.Stop() //nolint:errcheck // service stop on shutdown is best-effort
		}

		t.microServices = nil
		t.mu.Unlock()

		t.mu.Lock()
		subs := t.subs
		t.subs = nil
		t.mu.Unlock()

		for _, s := range subs {
			_ = s.Unsubscribe() //nolint:errcheck // unsubscribe on shutdown is best-effort
		}

		t.workersWg.Wait()

		t.mu.Lock()
		nc.Close()

		t.conn = nil
		t.js = nil
		t.mu.Unlock()
	}()

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
	t.log.Info("nats_transport_started", "url", sanitizeURL(activeURL))

	closedChan := nc.StatusChanged(nats.CLOSED)
	select {
	case <-runCtx.Done():
		t.log.Info("nats_transport_shutting_down")

		return nil, nil //nolint:nilnil // graceful shutdown: no result, no error
	case <-closedChan:
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, nil //nolint:nilnil // caller canceled ctx: no result, no error
		}

		return nil, xerr.Unavailable("nats: connection closed", nc.LastError())
	}
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
				err = t.subscribeTopic(ctx, nc, ex, binding, meta)
			case RequestBinding:
				err = t.subscribeTopic(ctx, nc, ex, TopicBinding{Subject: binding.Subject}, meta)
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
				t.log.Warn("skipping_unsupported_binding", "binding", fmt.Sprint(b))
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
	b TopicBinding, meta *action.Meta,
) error {
	handler := func(msg *nats.Msg) {
		t.workersWg.Add(1)
		go func(m *nats.Msg) {
			defer t.workersWg.Done()

			reqCtx, scope, release := xctx.NewScope(ctx)
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
					replyBytes, _ = encodeError(t.codec, execErr) //nolint:errcheck // original err is returned to caller
					err = execErr
				} else {
					replyBytes, err = t.codec.Marshal(res)
					if err != nil {
						replyBytes, _ = encodeError(t.codec, xerr.Internal("failed to marshal response", err)) //nolint:errcheck // original err is returned to caller
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

				_ = nc.PublishMsg(reply) //nolint:errcheck // reply is best-effort; failure surfaces as requester timeout
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
