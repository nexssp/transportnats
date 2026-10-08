package tnats

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/transport/identity"
)

// natsSource adapts nats.Header to identity.Source.
type natsSource struct{ h nats.Header }

func (s natsSource) Header(name string) string { return s.h.Get(name) }

// natsWriter adapts nats.Header to identity.Writer.
type natsWriter struct{ h nats.Header }

func (w natsWriter) SetHeader(name, value string) { w.h.Set(name, value) }

// populateFromMsg is called by the NATS subscriber on ingress.
func populateFromMsg(msg *nats.Msg) context.Context {
	if msg == nil {
		return context.Background()
	}
	return identity.Populate(context.Background(), natsSource{h: msg.Header})
}

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
