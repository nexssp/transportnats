package tnats

import (
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/transport/codec"
)

// FromConn wraps a caller-supplied *nats.Conn in a Transport. The
// transport uses the given connection as-is and never dials a new one.
// Close() does not close the caller's connection.
func FromConn(nc *nats.Conn) *Transport {
	t := &Transport{
		codec:          codec.Default,
		idemStore:      action.NewMemoryIdempotencyStore(0),
		fetchBatchSize: 1,
		fetchTimeout:   time.Second,
		ready:          make(chan struct{}),
		log:            slog.Default().With("transport", "nats"),
	}
	if nc != nil {
		t.conn = nc
		t.url = nc.ConnectedUrl()
		if js, err := nc.JetStream(); err == nil {
			t.js = js
		}
	}
	close(t.ready)
	return t
}
