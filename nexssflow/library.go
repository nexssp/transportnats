package nexssflow

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"

	"github.com/nexssp/flow/contracts"
	"github.com/nexssp/flow/core"

	"github.com/nexssp/transportnats/tnats"
)

const ID = "transportnats"

func init() {
	core.Register(ID, Bundle)
}

// Config controls one Flow-owned NATS transport instance.
type Config struct {
	URL            string        `flow:"url"              default:"nats://127.0.0.1:4222"`
	Name           string        `flow:"name"             default:"nflow-client"`
	Timeout        time.Duration `flow:"timeout"          default:"5s"`
	CredsFile      string        `flow:"creds_file"`
	Token          string        `flow:"token"`
	RootCAFile     string        `flow:"root_ca_file"`
	ClientCertFile string        `flow:"client_cert_file"`
	ClientKeyFile  string        `flow:"client_key_file"`
	Production     bool          `flow:"production"`
}

func Bundle(rawOpts map[string]string) core.Bundle {
	cfg, err := core.Decode[Config](rawOpts)
	if err != nil {
		panic("transportnats bundle: " + err.Error())
	}

	bundle, err := NewBundle(cfg)
	if err != nil {
		panic("transportnats bundle: " + err.Error())
	}
	return bundle
}

// NewBundle builds a configured Flow extension and returns configuration
// errors instead of panicking. Production mode validates TLS and credentials
// before the transport can be used.
func NewBundle(cfg Config) (core.Bundle, error) {
	if cfg.URL == "" {
		cfg.URL = "nats://127.0.0.1:4222"
	}
	if cfg.Name == "" {
		cfg.Name = "nflow-client"
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.CredsFile != "" && cfg.Token != "" {
		return core.Bundle{}, errors.New("configure only one of creds_file or token")
	}
	if (cfg.ClientCertFile == "") != (cfg.ClientKeyFile == "") {
		return core.Bundle{}, errors.New("client_cert_file and client_key_file must be configured together")
	}

	opts := []tnats.Option{tnats.WithNATSOptions(nats.Name(cfg.Name))}
	if cfg.Production {
		opts = append(opts, tnats.WithProductionSecurity(tnats.ProductionSecurity{
			RootCAFile:      cfg.RootCAFile,
			CredentialsFile: cfg.CredsFile,
			ClientCertFile:  cfg.ClientCertFile,
			ClientKeyFile:   cfg.ClientKeyFile,
		}))
	} else {
		if cfg.CredsFile != "" {
			opts = append(opts, tnats.WithJWT(cfg.CredsFile))
		}
		if cfg.Token != "" {
			opts = append(opts, tnats.WithToken(cfg.Token))
		}
		if cfg.RootCAFile != "" || cfg.ClientCertFile != "" {
			opts = append(opts, tnats.WithTLS(cfg.RootCAFile, cfg.ClientCertFile, cfg.ClientKeyFile))
		}
	}

	tr := tnats.New(cfg.URL, opts...)
	if cfg.Production {
		if err := tr.ValidateProduction(); err != nil {
			return core.Bundle{}, fmt.Errorf("validate production NATS settings: %w", err)
		}
	}

	return core.Bundle{
		ID:        ID,
		Libraries: []action.Library{Library(tr, cfg)},
		Modifiers: Modifiers(),
		Shutdowns: []core.ShutdownFunc{
			func(_ context.Context) error {
				return tr.Close()
			},
		},
	}, nil
}

func Library(tr *tnats.Transport, cfg Config) action.Library {
	if tr == nil {
		tr = tnats.New(cfg.URL)
	}
	return action.Library{
		Name: ID,
		Actions: []action.AnyAction{
			listenAction(tr),
			publishAction(tr, cfg),
			requestAction(tr, cfg),
			publishDurableAction(tr, cfg),
		},
	}
}

func Modifiers() []core.Modifier {
	return []core.Modifier{
		core.String("nats_topic", func(b *action.Builder[any, any], raw string) *action.Builder[any, any] {
			subject, queue, _ := strings.Cut(raw, "@")
			return b.Route(tnats.Topic(strings.TrimSpace(subject), strings.TrimSpace(queue)))
		}),

		core.String("nats_request", func(b *action.Builder[any, any], subject string) *action.Builder[any, any] {
			return b.Route(tnats.Request(strings.TrimSpace(subject)))
		}),

		core.String("nats_durable", func(b *action.Builder[any, any], raw string) *action.Builder[any, any] {
			parts := strings.Split(raw, ":")
			if len(parts) != 4 {
				return b.Route(invalidBinding("@nats_durable expects stream:subject:durable:dlq"))
			}
			return b.Route(tnats.DurableWork(parts[0], parts[1], parts[2], parts[3]))
		}),

		core.String("nats_consumer", func(b *action.Builder[any, any], raw string) *action.Builder[any, any] {
			parts := strings.Split(raw, ":")
			if len(parts) != 3 {
				return b.Route(invalidBinding("@nats_consumer expects stream:subject:durable"))
			}
			return b.Route(tnats.Consumer(parts[0], parts[1], parts[2]))
		}),

		core.String("nats_kv", func(b *action.Builder[any, any], raw string) *action.Builder[any, any] {
			bucket, key, _ := strings.Cut(raw, ":")
			return b.Route(tnats.KV(strings.TrimSpace(bucket), strings.TrimSpace(key)))
		}),

		core.String("nats_object", func(b *action.Builder[any, any], raw string) *action.Builder[any, any] {
			bucket, pattern, _ := strings.Cut(raw, ":")
			return b.Route(tnats.ObjectStore(strings.TrimSpace(bucket), strings.TrimSpace(pattern)))
		}),

		core.String("nats_micro", func(b *action.Builder[any, any], raw string) *action.Builder[any, any] {
			parts := strings.Split(raw, ":")
			if len(parts) != 4 {
				return b.Route(invalidBinding("@nats_micro expects service:version:endpoint:subject"))
			}
			return b.Route(tnats.Service(parts[0], parts[1], parts[2], parts[3]))
		}),
	}
}

type invalidBinding string

func (b invalidBinding) String() string { return string(b) }

type listenReq struct {
	Endpoints []string `json:"endpoints"`
}

func listenAction(tr *tnats.Transport) action.AnyAction {
	return action.New("nats.listen", func(ctx context.Context, in any) (any, error) {
		var req listenReq
		if m, ok := in.(map[string]any); ok {
			if eps, ok := m["endpoints"].([]any); ok {
				for _, e := range eps {
					if s, ok := e.(string); ok {
						req.Endpoints = append(req.Endpoints, s)
					}
				}
			}
		}

		resolver := contracts.ActionResolverFromContext(ctx)
		if resolver == nil {
			return nil, errors.New("nats.listen: action resolver is nil")
		}

		var actionsToMount []action.AnyAction

		if len(req.Endpoints) > 0 {
			for _, name := range req.Endpoints {
				act, ok := resolver.Action(name)
				if !ok {
					return nil, xerr.NotFound("nats.listen: endpoint not found: " + name)
				}
				actionsToMount = append(actionsToMount, act)
			}
		} else {
			if lister, ok := resolver.(interface{ Actions() []action.AnyAction }); ok {
				actionsToMount = filterNATSBoundActions(lister.Actions())
			}
			if len(actionsToMount) == 0 {
				return nil, errors.New("nats.listen: no actions found")
			}
		}

		for _, act := range actionsToMount {
			for _, b := range act.GetBindings() {
				if bad, ok := b.(invalidBinding); ok {
					name := ""
					if meta := act.Describe(); meta != nil {
						name = meta.Name
					}
					return nil, xerr.BadRequest(fmt.Sprintf("nats.listen: action %q has invalid binding: %s", name, string(bad)))
				}
			}
		}

		tr.Mount(actionsToMount)
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		done := make(chan error, 1)
		go func() {
			_, err := tr.Do(runCtx, nil)
			done <- err
		}()

		if err := tr.WaitReady(runCtx); err != nil {
			cancel()
			doErr := <-done
			if errors.Is(ctx.Err(), context.Canceled) {
				return nil, nil
			}
			if doErr != nil {
				return nil, xerr.Unavailable("nats.listen: transport failed to start", doErr)
			}
			return nil, xerr.Unavailable("nats.listen: timeout waiting for connection", err)
		}

		err := <-done
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, nil
		}
		return nil, err
	}).
		Tag("transport", "nats").
		Build()
}

var natsBindingCheckers = []func(action.Binding) bool{
	func(b action.Binding) bool { _, ok := b.(tnats.TopicBinding); return ok },
	func(b action.Binding) bool { _, ok := b.(tnats.RequestBinding); return ok },
	func(b action.Binding) bool { _, ok := b.(tnats.DurableBinding); return ok },
	func(b action.Binding) bool { _, ok := b.(tnats.ConsumerBinding); return ok },
	func(b action.Binding) bool { _, ok := b.(tnats.KVBinding); return ok },
	func(b action.Binding) bool { _, ok := b.(tnats.ObjectBinding); return ok },
	func(b action.Binding) bool { _, ok := b.(tnats.ServiceBinding); return ok },
}

func filterNATSBoundActions(actions []action.AnyAction) []action.AnyAction {
	var out []action.AnyAction
	for _, act := range actions {
		if act == nil {
			continue
		}
		if slices.ContainsFunc(act.GetBindings(), func(b action.Binding) bool {
			return slices.ContainsFunc(natsBindingCheckers, func(f func(action.Binding) bool) bool { return f(b) })
		}) {
			out = append(out, act)
		}
	}
	return out
}

type PublishReq struct {
	Subject string `json:"subject"`
	Payload any    `json:"payload,omitempty"`
}

func publishAction(tr *tnats.Transport, cfg Config) action.AnyAction {
	return action.New("nats.publish", func(ctx context.Context, req PublishReq) (any, error) {
		if req.Subject == "" {
			return nil, xerr.BadRequest("nats.publish: subject is required")
		}
		callCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()

		if err := tr.Publish(callCtx, req.Subject, req.Payload); err != nil {
			return nil, err
		}
		return map[string]any{"status": "published", "subject": req.Subject}, nil
	}).Description("Publish a payload to a NATS subject").Tag("transport", "nats").Build()
}

type RequestReq struct {
	Subject string `json:"subject"`
	Payload any    `json:"payload,omitempty"`
}

func requestAction(tr *tnats.Transport, cfg Config) action.AnyAction {
	return action.New("nats.request", func(ctx context.Context, req RequestReq) (any, error) {
		if req.Subject == "" {
			return nil, xerr.BadRequest("nats.request: subject is required")
		}
		callCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()

		var out any
		if err := tr.Request(callCtx, req.Subject, req.Payload, &out); err != nil {
			return nil, err
		}
		return out, nil
	}).Description("Send a NATS request and await the reply").Tag("transport", "nats").Build()
}

type PublishDurableReq struct {
	Stream            string `json:"stream"`
	Subject           string `json:"subject"`
	Durable           string `json:"durable"`
	DeadLetterSubject string `json:"dead_letter_subject"`
	Payload           any    `json:"payload,omitempty"`
}

func publishDurableAction(tr *tnats.Transport, cfg Config) action.AnyAction {
	return action.New("nats.publish_durable", func(ctx context.Context, req PublishDurableReq) (any, error) {
		if req.Stream == "" || req.Subject == "" || req.Durable == "" || req.DeadLetterSubject == "" {
			return nil, xerr.BadRequest("nats.publish_durable: stream, subject, durable, and dlq are required")
		}
		callCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()

		binding := tnats.DurableWork(req.Stream, req.Subject, req.Durable, req.DeadLetterSubject)
		if err := tr.PublishDurable(callCtx, binding, req.Payload); err != nil {
			return nil, err
		}
		return map[string]any{"status": "durable", "subject": req.Subject, "stream": req.Stream}, nil
	}).Description("Enqueue a JetStream durable work item").Tag("transport", "nats").Build()
}
