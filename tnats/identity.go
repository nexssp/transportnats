package tnats

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/transport/identity"
)

// natsWriter adapts nats.Header to identity.Writer.
type natsWriter struct{ h nats.Header }

func (w natsWriter) SetHeader(name, value string) { w.h.Set(name, value) }

// propagateToMsg is called by the NATS publisher on egress.
func propagateToMsg(ctx context.Context, msg *nats.Msg) {
	if msg == nil {
		return
	}
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	identity.Propagate(ctx, natsWriter{h: msg.Header})
}
