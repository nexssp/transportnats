package tnats

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/xerr"
)

type cachedConnectionState struct {
	conn  *nats.Conn
	err   error
	ready bool
}

// EnsureConn returns the transport connection, dialing lazily on first
// use. Safe for concurrent callers. The returned connection is never nil
// on success.
//
// The transport may have been constructed with a preexisting connection
// (FromConn); in that case EnsureConn returns it immediately and never
// dials. Otherwise the connection is dialed with the transport's stored
// URL and NATS options, and cached for the lifetime of the transport.
func (t *Transport) EnsureConn(ctx context.Context) (*nats.Conn, error) {
	if ctx == nil {
		return nil, xerr.BadRequest("nats: context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if state := t.cachedConnection(); state.ready {
		return state.conn, state.err
	}

	if err := t.lockConnect(ctx); err != nil {
		return nil, err
	}
	defer t.unlockConnect()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if state := t.cachedConnection(); state.ready {
		return state.conn, state.err
	}

	return t.dialConnection(ctx)
}

func (t *Transport) cachedConnection() cachedConnectionState {
	t.mu.RLock()
	nc := t.conn
	initErr := t.initErr
	providedConn := t.providedConn
	closing := t.closing
	t.mu.RUnlock()
	if initErr != nil {
		return cachedConnectionState{err: MapError(initErr), ready: true}
	}
	if closing {
		return cachedConnectionState{err: xerr.Unavailable("nats: transport is closed"), ready: true}
	}
	if providedConn {
		if nc == nil || nc.IsClosed() {
			return cachedConnectionState{err: xerr.Unavailable("nats: caller-supplied connection is closed"), ready: true}
		}
		return cachedConnectionState{conn: nc, ready: true}
	}
	if nc != nil && !nc.IsClosed() {
		return cachedConnectionState{conn: nc, ready: true}
	}
	return cachedConnectionState{}
}

func (t *Transport) dialConnection(ctx context.Context) (*nats.Conn, error) {
	t.mu.RLock()
	rawURL := t.url
	options := append([]nats.Option(nil), t.options...)
	t.mu.RUnlock()
	if rawURL == "" {
		return nil, xerr.Internal("nats: URL is required to dial")
	}
	connectCtx, cancel := context.WithCancel(ctx)
	t.mu.Lock()
	if t.closing {
		t.mu.Unlock()
		cancel()
		return nil, xerr.Unavailable("nats: transport is closed")
	}
	t.connectCancel = cancel
	t.mu.Unlock()
	defer func() {
		t.mu.Lock()
		t.connectCancel = nil
		t.mu.Unlock()
		cancel()
	}()

	connected := make(chan struct{}, 1)
	closed := make(chan struct{}, 1)

	opts := append([]nats.Option{
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectBufSize(8 * 1024 * 1024),
		nats.Timeout(3 * time.Second),
	}, options...)
	opts = append(opts, connectionCallbacks(connected, closed))

	dialed, err := nats.Connect(rawURL, opts...)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, MapError(err)
	}
	if err := waitForInitialConnection(ctx, connectCtx, dialed, connected, closed); err != nil {
		dialed.Close()
		return nil, err
	}

	js, jsErr := dialed.JetStream()
	if jsErr != nil {
		dialed.Close()
		return nil, MapError(jsErr)
	}

	t.mu.Lock()
	if t.closing {
		t.mu.Unlock()
		dialed.Close()
		return nil, xerr.Unavailable("nats: transport closed during connection setup")
	}
	t.conn = dialed
	t.js = js
	t.mu.Unlock()

	return dialed, nil
}

func connectionCallbacks(connected, closed chan struct{}) nats.Option {
	return func(options *nats.Options) error {
		connectedCallback := options.ConnectedCB
		reconnectedCallback := options.ReconnectedCB
		closedCallback := options.ClosedCB
		signal := func(ch chan struct{}) {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
		options.ConnectedCB = func(nc *nats.Conn) {
			signal(connected)
			if connectedCallback != nil {
				connectedCallback(nc)
			}
		}
		options.ReconnectedCB = func(nc *nats.Conn) {
			signal(connected)
			if reconnectedCallback != nil {
				reconnectedCallback(nc)
			}
		}
		options.ClosedCB = func(nc *nats.Conn) {
			signal(closed)
			if closedCallback != nil {
				closedCallback(nc)
			}
		}
		return nil
	}
}

func waitForInitialConnection(
	ctx, connectCtx context.Context, nc *nats.Conn, connected, closed <-chan struct{},
) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if connectCtx.Err() != nil {
			return xerr.Unavailable("nats: transport closed during initial connection")
		}
		if nc.IsConnected() {
			return nil
		}
		select {
		case <-connected:
		case <-closed:
			if err := nc.LastError(); err != nil {
				return MapError(err)
			}
			return xerr.Unavailable("nats: connection closed before initial connection")
		case <-ctx.Done():
			return ctx.Err()
		case <-connectCtx.Done():
			return xerr.Unavailable("nats: transport closed during initial connection")
		}
	}
}

func (t *Transport) lockConnect(ctx context.Context) error {
	t.connectGateOnce.Do(func() {
		t.connectGate = make(chan struct{}, 1)
	})

	select {
	case t.connectGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *Transport) unlockConnect() {
	<-t.connectGate
}

// Close releases the transport connection if one was dialed.
// Safe to call multiple times.
func (t *Transport) Close() error {
	t.mu.Lock()
	t.closing = true
	cancelConnect := t.connectCancel
	t.mu.Unlock()
	if cancelConnect != nil {
		cancelConnect()
	}

	t.connectGateOnce.Do(func() {
		t.connectGate = make(chan struct{}, 1)
	})
	t.connectGate <- struct{}{}
	defer t.unlockConnect()

	t.mu.Lock()
	nc := t.conn
	providedConn := t.providedConn
	t.conn = nil
	t.js = nil
	t.mu.Unlock()
	if nc != nil && !providedConn {
		nc.Close()
	}
	return nil
}

// Connected reports whether EnsureConn has already succeeded.
func (t *Transport) Connected() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.conn != nil && !t.conn.IsClosed()
}
