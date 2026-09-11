package tnats

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/transport"
)

// ServiceBinding udostępnia akcję jako endpoint w standardzie NATS Micro Services.
type ServiceBinding struct {
	Service     string
	Version     string
	Description string
	Endpoint    string
	Subject     string
	QueueGroup  string
	Timeout     time.Duration
	Metadata    map[string]string
}

func (b ServiceBinding) String() string {
	return "nats micro: " + b.Service + "@" + b.Version + " -> " + b.Subject
}

func Service(service, version, endpoint, subject string) ServiceBinding {
	return ServiceBinding{
		Service:     service,
		Version:     version,
		Endpoint:    endpoint,
		Subject:     subject,
		Description: "Nexss action " + endpoint,
	}
}

func (b ServiceBinding) WithQueueGroup(qg string) ServiceBinding {
	b.QueueGroup = qg

	return b
}

func (b ServiceBinding) WithTimeout(d time.Duration) ServiceBinding {
	b.Timeout = d

	return b
}

func (b ServiceBinding) WithMetadata(md map[string]string) ServiceBinding {
	cp := make(map[string]string, len(md))
	for k, v := range md {
		cp[k] = v
	}

	b.Metadata = cp

	return b
}

type microEndpoint struct {
	Binding ServiceBinding
	Exec    action.Executable
	Meta    *action.Meta
}

func (t *Transport) mountMicroServices(ctx context.Context, nc *nats.Conn, actions []action.AnyAction) error {
	groups := make(map[string][]microEndpoint)
	order := make([]string, 0)

	for _, act := range actions {
		exec, ok := act.(action.Executable)
		if !ok {
			continue
		}

		meta := act.Describe()
		for _, b := range act.GetBindings() {
			sb, ok := b.(ServiceBinding)
			if !ok {
				continue
			}

			if _, seen := groups[sb.Service]; !seen {
				order = append(order, sb.Service)
			}

			groups[sb.Service] = append(groups[sb.Service], microEndpoint{
				Binding: sb,
				Exec:    exec,
				Meta:    meta,
			})
		}
	}

	for _, name := range order {
		eps := groups[name]
		head := eps[0].Binding

		svc, err := micro.AddService(nc, micro.Config{
			Name:        head.Service,
			Version:     head.Version,
			Description: head.Description,
			Metadata:    head.Metadata,
		})
		if err != nil {
			return MapError(err)
		}

		for i := range eps {
			if err := t.registerMicroEndpoint(ctx, svc, eps[i]); err != nil {
				_ = svc.Stop()

				return err
			}
		}

		t.mu.Lock()
		t.microServices = append(t.microServices, svc)
		t.mu.Unlock()
		t.log.Info(
			"nats_micro_service_started", "service", head.Service,
			"version", head.Version, "endpoints", len(eps),
		)
	}

	return nil
}

func (t *Transport) registerMicroEndpoint(ctx context.Context, svc micro.Service, ep microEndpoint) error {
	b := ep.Binding
	handler := func(req micro.Request) {
		reqCtx, scope, release := xctx.NewScope(ctx)
		defer release()

		scope.Endpoint = "nats.micro." + b.Service + "." + b.Endpoint
		if reqID := req.Headers().Get(transport.HeaderRequestID); reqID != "" {
			scope.RequestID = reqID
			reqCtx = xctx.WithRequestID(reqCtx, reqID)
		}

		if traceID := req.Headers().Get(transport.HeaderTraceID); traceID != "" {
			spanID := req.Headers().Get(transport.HeaderSpanID)
			reqCtx = action.WithTraceContext(reqCtx, traceID, spanID)
		}

		if b.Timeout > 0 {
			var cancel context.CancelFunc

			reqCtx, cancel = context.WithTimeout(reqCtx, b.Timeout)
			defer cancel()
		}

		decoder := func(v any) error {
			if len(req.Data()) == 0 {
				return nil
			}

			return t.codec.Unmarshal(req.Data(), v)
		}

		res, execErr := ep.Exec.ExecuteDecoded(reqCtx, decoder)
		if execErr != nil {
			appErr := xerr.From(execErr)
			_ = req.Error(string(appErr.Kind), appErr.Error(), nil)

			return
		}

		payload, mErr := t.codec.Marshal(res)
		if mErr != nil {
			_ = req.Error(string(xerr.KindInternal), "response marshal failed", nil)

			return
		}

		_ = req.Respond(payload)
	}

	opts := []micro.EndpointOpt{
		micro.WithEndpointSubject(b.Subject),
	}
	if b.QueueGroup != "" {
		opts = append(opts, micro.WithEndpointQueueGroup(b.QueueGroup))
	}

	if len(b.Metadata) > 0 {
		opts = append(opts, micro.WithEndpointMetadata(b.Metadata))
	}

	return svc.AddEndpoint(b.Endpoint, micro.HandlerFunc(handler), opts...)
}

var _ action.Binding = ServiceBinding{}
