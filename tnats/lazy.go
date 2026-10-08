package tnats

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/xerr"
)

// EnsureConn returns the transport connection, dialing lazily on first
// use. Safe for concurrent callers. The returned connection is never nil
// on success.
//
// The transport may have been constructed with a preexisting connection
// (FromConn); in that case EnsureConn returns it immediately and never
// dials. Otherwise the connection is dialed with the transport's stored
// URL and NATS options, and cached for the lifetime of the transport.
func (t *Transport) EnsureConn(ctx context.Context) (*nats.Conn, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}

	t.mu.RLock()
	nc := t.conn
	t.mu.RUnlock()
	if nc != nil && !nc.IsClosed() {
		return nc, nil
	}

	t.connectMu.Lock()
	defer t.connectMu.Unlock()

	// Double-check after acquiring the write lock.
	t.mu.RLock()
	nc = t.conn
	rawURL := t.url
	t.mu.RUnlock()
	if nc != nil && !nc.IsClosed() {
		return nc, nil
	}

	if rawURL == "" {
		return nil, xerr.Internal("nats: URL is required to dial")
	}

	opts := append([]nats.Option{
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectBufSize(8 * 1024 * 1024),
		nats.Timeout(3 * time.Second),
	}, t.options...)

	dialed, err := nats.Connect(rawURL, opts...)
	if err != nil {
		return nil, MapError(err)
	}

	js, jsErr := dialed.JetStream()
	if jsErr != nil {
		dialed.Close()
		return nil, MapError(jsErr)
	}

	t.mu.Lock()
	t.conn = dialed
	t.js = js
	t.mu.Unlock()

	return dialed, nil
}

// Close releases the transport connection if one was dialed.
// Safe to call multiple times.
func (t *Transport) Close() error {
	t.mu.Lock()
	nc := t.conn
	t.conn = nil
	t.js = nil
	t.mu.Unlock()
	if nc != nil {
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
