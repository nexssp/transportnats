package tnats

import (
	"context"
	"net/url"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/transport"
)

func sanitizeURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}

	return u.Redacted()
}

func injectContextHeaders(ctx context.Context, msg *nats.Msg) {
	reqID := xctx.RequestIDFrom(ctx)
	execID := xctx.ExecutionIDFrom(ctx)
	traceID := xctx.TraceIDFrom(ctx)
	spanID := xctx.SpanIDFrom(ctx)

	if reqID == "" && execID == "" && traceID == "" && spanID == "" {
		return
	}

	if msg.Header == nil {
		msg.Header = make(nats.Header, 4)
	}

	if reqID != "" {
		msg.Header.Set(transport.HeaderRequestID, reqID)
	}

	if execID != "" {
		msg.Header.Set(transport.HeaderExecutionID, execID)
	}

	if traceID != "" {
		msg.Header.Set(transport.HeaderTraceID, traceID)
	}

	if spanID != "" {
		msg.Header.Set(transport.HeaderSpanID, spanID)
	}
}

func extractContextHeaders(ctx context.Context, msg *nats.Msg, scope *xctx.RequestScope) context.Context {
	if msg.Header == nil {
		return ctx
	}

	if reqID := msg.Header.Get(transport.HeaderRequestID); reqID != "" {
		scope.RequestID = reqID
		ctx = xctx.WithRequestID(ctx, reqID)
	}

	if execID := msg.Header.Get(transport.HeaderExecutionID); execID != "" {
		scope.ExecutionID = execID
		ctx = xctx.WithExecutionID(ctx, execID)
	}

	if traceID := msg.Header.Get(transport.HeaderTraceID); traceID != "" {
		scope.TraceID = traceID
		ctx = xctx.WithTraceID(ctx, traceID)
	}

	if spanID := msg.Header.Get(transport.HeaderSpanID); spanID != "" {
		scope.SpanID = spanID
		ctx = xctx.WithSpanID(ctx, spanID)
	}

	return ctx
}
