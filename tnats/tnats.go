package tnats

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/transport"
	"github.com/nexssp/transport/codec"
)

const defaultShutdownTimeout = 5 * time.Second

type DeadLetter struct {
	Stream     string            `json:"stream"`
	Subject    string            `json:"subject"`
	Durable    string            `json:"durable"`
	Deliveries uint64            `json:"deliveries"`
	FailedAt   time.Time         `json:"failed_at"`
	Error      string            `json:"error"`
	Payload    []byte            `json:"payload"`
	Headers    map[string]string `json:"headers,omitempty"`
}

type Option func(*Transport)

type ProductionSecurity struct {
	CredentialsFile string
	RootCAFile      string
	ClientCertFile  string
	ClientKeyFile   string
}

type Transport struct {
	url                  string
	conn                 *nats.Conn
	js                   nats.JetStreamContext
	embedded             *EmbeddedConfig
	embeddedSrv          *natsserver.Server
	actions              []action.AnyAction
	codec                codec.Codec
	idemStore            action.IdempotencyStore
	microServices        []micro.Service
	options              []nats.Option
	productionSecurity   *ProductionSecurity
	productionValidator  func() error
	streamConfigModifier func(*nats.StreamConfig)
	fetchBatchSize       int
	fetchTimeout         time.Duration
	ready                chan struct{}
	readyErr             error
	readyOnce            sync.Once
	initErr              error
	log                  *slog.Logger
	subs                 []*nats.Subscription
	workersWg            sync.WaitGroup
	verifiedStreams      sync.Map
	mu                   sync.RWMutex
	running              atomic.Bool
}

var _ transport.Transport = (*Transport)(nil)

func (t *Transport) CanHandle(b action.Binding) bool {
	switch b.(type) {
	case TopicBinding, KVBinding, RequestBinding, DurableBinding,
		ConsumerBinding, ObjectBinding, ServiceBinding:
		return true
	default:
		return false
	}
}

func New(rawURL string, opts ...Option) *Transport {
	t := &Transport{
		url:            rawURL,
		codec:          codec.Default,
		idemStore:      action.NewMemoryIdempotencyStore(0),
		fetchBatchSize: 1,
		fetchTimeout:   time.Second,
		ready:          make(chan struct{}),
		log:            slog.Default().With("transport", "nats"),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(t)
		}
	}

	return t
}

func WithCodec(c codec.Codec) Option {
	return func(t *Transport) {
		if c != nil {
			t.codec = c
		}
	}
}

func WithIdempotencyStore(s action.IdempotencyStore) Option {
	return func(t *Transport) {
		if s != nil {
			t.idemStore = s
		}
	}
}

func WithNATSOptions(options ...nats.Option) Option {
	return func(t *Transport) {
		t.options = append(t.options, options...)
	}
}

func WithLogger(log *slog.Logger) Option {
	return func(t *Transport) {
		if log != nil {
			t.log = log.With("transport", "nats")
		}
	}
}

func WithFetchBatch(batchSize int, timeout time.Duration) Option {
	return func(t *Transport) {
		if batchSize > 0 {
			t.fetchBatchSize = batchSize
		}

		if timeout > 0 {
			t.fetchTimeout = timeout
		}
	}
}

func WithStreamConfigModifier(modifier func(*nats.StreamConfig)) Option {
	return func(t *Transport) {
		t.streamConfigModifier = modifier
	}
}

func WithProductionSecurity(security ProductionSecurity) Option {
	return func(t *Transport) {
		cp := security

		t.productionSecurity = &cp
		if security.RootCAFile != "" {
			t.options = append(t.options, nats.RootCAs(security.RootCAFile))
		}

		if security.ClientCertFile != "" && security.ClientKeyFile != "" {
			t.options = append(t.options, nats.ClientCert(security.ClientCertFile, security.ClientKeyFile))
		}

		if security.CredentialsFile != "" {
			t.options = append(t.options, nats.UserCredentials(security.CredentialsFile))
		}

		t.options = append(t.options, nats.Secure())
	}
}

func WithProductionValidation(validate func() error) Option {
	return func(t *Transport) {
		t.productionValidator = validate
	}
}

func (t *Transport) String() string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.embedded != nil {
		return "nats(embedded)"
	}

	return "nats(" + sanitizeURL(t.url) + ")"
}

func (t *Transport) Conn() *nats.Conn {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.conn
}

func (t *Transport) JetStream() nats.JetStreamContext {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.js
}

func (t *Transport) Mount(actions []action.AnyAction) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.actions = append([]action.AnyAction(nil), actions...)
}

func (t *Transport) WaitReady(ctx context.Context) error {
	t.mu.RLock()
	readyCh := t.ready
	t.mu.RUnlock()

	select {
	case <-readyCh:
		t.mu.RLock()
		defer t.mu.RUnlock()

		return t.readyErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *Transport) signalReady(err error) {
	t.readyOnce.Do(func() {
		t.mu.Lock()
		t.readyErr = err
		close(t.ready)
		t.mu.Unlock()
	})
}

func (t *Transport) AsAction() *action.Builder[any, any] {
	return action.New("transport.nats."+sanitizeURL(t.url), func(ctx context.Context, _ any) (any, error) {
		return t.Do(ctx, nil)
	}).
		Tag("infra", "transport", "nats").
		Description("NATS Transport Runner on " + sanitizeURL(t.url))
}
