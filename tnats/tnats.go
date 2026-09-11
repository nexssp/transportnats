package tnats

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
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

func (t *Transport) Publish(ctx context.Context, subject string, payload any) error {
	nc := t.Conn()
	if nc == nil {
		return xerr.Unavailable("nats: connection not ready")
	}

	data, err := t.codec.Marshal(payload)
	if err != nil {
		return xerr.Internal("failed to marshal nats payload", err)
	}

	msg := nats.NewMsg(subject)
	msg.Data = data
	injectContextHeaders(ctx, msg)

	if pubErr := nc.PublishMsg(msg); pubErr != nil {
		return MapError(pubErr)
	}

	return nil
}

func (t *Transport) PublishDurable(ctx context.Context, binding DurableBinding, payload any) error {
	if err := validateDurableBinding(binding); err != nil {
		return err
	}

	js, err := t.ensureJetStream()
	if err != nil {
		return err
	}

	cacheKey := binding.Stream + ":" + binding.Subject
	if _, ok := t.verifiedStreams.Load(cacheKey); !ok {
		if infraErr := t.ensureDurableInfra(js, binding); infraErr != nil {
			return infraErr
		}

		t.verifiedStreams.Store(cacheKey, struct{}{})
	}

	data, marshalErr := t.codec.Marshal(payload)
	if marshalErr != nil {
		return xerr.Internal("failed to marshal durable nats payload", marshalErr)
	}

	msg := nats.NewMsg(binding.Subject)
	msg.Data = data
	injectContextHeaders(ctx, msg)

	if _, pubErr := js.PublishMsg(msg, nats.Context(ctx)); pubErr != nil {
		return MapError(pubErr)
	}

	return nil
}

func (t *Transport) Request(ctx context.Context, subject string, payload any, resPtr any) error {
	nc := t.Conn()
	if nc == nil {
		return xerr.Unavailable("nats: connection not ready")
	}

	data, err := t.codec.Marshal(payload)
	if err != nil {
		return xerr.Internal("failed to marshal nats request", err)
	}

	reqMsg := nats.NewMsg(subject)
	reqMsg.Data = data
	injectContextHeaders(ctx, reqMsg)

	resMsg, reqErr := nc.RequestMsgWithContext(ctx, reqMsg)
	if reqErr != nil {
		return MapError(reqErr)
	}

	if resMsg.Header.Get(HeaderErrorMarker) == "1" {
		var errResp xerr.ErrorResponse
		if jsonErr := t.codec.Unmarshal(resMsg.Data, &errResp); jsonErr == nil && errResp.Error != "" {
			if appErr, ok := xerr.FromPublic(errResp); ok {
				return appErr
			}
		}

		return xerr.Internal(fmt.Sprintf("remote nats action error: %s", string(resMsg.Data)))
	}

	if resPtr != nil && len(resMsg.Data) > 0 {
		if unmarshalErr := t.codec.Unmarshal(resMsg.Data, resPtr); unmarshalErr != nil {
			return xerr.Internal("failed to unmarshal nats response", unmarshalErr)
		}
	}

	return nil
}

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
			_ = svc.Stop()
		}

		t.microServices = nil
		t.mu.Unlock()

		t.mu.Lock()
		subs := t.subs
		t.subs = nil
		t.mu.Unlock()

		for _, s := range subs {
			_ = s.Unsubscribe()
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

		return nil, nil
	case <-closedChan:
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, nil
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
					replyBytes, _ = encodeError(t.codec, execErr)
					err = execErr
				} else {
					replyBytes, err = t.codec.Marshal(res)
					if err != nil {
						replyBytes, _ = encodeError(t.codec, xerr.Internal("failed to marshal response", err))
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

				_ = nc.PublishMsg(reply)
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

func (t *Transport) mountKV(ctx context.Context, js nats.JetStreamContext, ex action.Executable, b KVBinding) error {
	kv, err := js.KeyValue(b.Bucket)
	if err != nil {
		return MapError(err)
	}

	t.workersWg.Add(1)
	go func() {
		defer t.workersWg.Done()

		backoff := 100 * time.Millisecond

		for {
			if ctx.Err() != nil {
				return
			}

			w, err := kv.Watch(b.Key, nats.IgnoreDeletes())
			if err != nil {
				t.log.Error("kv_watch_failed", "bucket", b.Bucket, "key", b.Key, "error", err)

				select {
				case <-time.After(backoff):
					if backoff < 5*time.Second {
						backoff *= 2
					}

					continue
				case <-ctx.Done():
					return
				}
			}

			backoff = 100 * time.Millisecond

			stopDone := make(chan struct{})

			go func() {
				select {
				case <-ctx.Done():
					_ = w.Stop()
				case <-stopDone:
				}
			}()

			for entry := range w.Updates() {
				if ctx.Err() != nil {
					break
				}

				if entry != nil {
					reqCtx, scope, release := xctx.NewScope(ctx)
					scope.Endpoint = "nats.kv." + b.Bucket + "." + b.Key

					decoder := func(v any) error { return t.codec.Unmarshal(entry.Value(), v) }
					if _, execErr := ex.ExecuteDecoded(reqCtx, decoder); execErr != nil {
						t.log.Error("kv_action_exec_failed", "bucket", b.Bucket, "key", b.Key, "error", execErr)
					}

					release()
				}
			}

			close(stopDone)

			_ = w.Stop()

			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

func (t *Transport) subscribeDurable(
	ctx context.Context, nc *nats.Conn, js nats.JetStreamContext,
	ex action.Executable, b DurableBinding, meta *action.Meta,
) error {
	if err := validateDurableBinding(b); err != nil {
		return err
	}

	if err := t.ensureDurableInfra(js, b); err != nil {
		return err
	}

	sub, err := js.PullSubscribe(b.Subject, b.Durable,
		nats.BindStream(b.Stream),
		nats.AckExplicit(),
		nats.AckWait(b.AckWait),
		nats.MaxDeliver(b.MaxDeliver),
		nats.MaxAckPending(b.MaxAckPending),
	)
	if err != nil {
		return MapError(err)
	}

	t.mu.Lock()
	t.subs = append(t.subs, sub)
	t.mu.Unlock()

	t.workersWg.Add(1)
	go t.consumeDurable(ctx, nc, js, sub, ex, b, meta)

	return nil
}

func (t *Transport) consumeDurable(
	ctx context.Context, nc *nats.Conn, js nats.JetStreamContext,
	sub *nats.Subscription, ex action.Executable, b DurableBinding, meta *action.Meta,
) {
	defer t.workersWg.Done()
	defer func() { _ = sub.Unsubscribe() }()

	for {
		if ctx.Err() != nil {
			return
		}

		fetchCtx, fetchCancel := context.WithTimeout(ctx, t.fetchTimeout)
		messages, err := sub.Fetch(t.fetchBatchSize, nats.Context(fetchCtx))

		fetchCancel()

		if err != nil {
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}

			if ctx.Err() != nil || errors.Is(err, context.Canceled) || nc.IsClosed() {
				return
			}

			select {
			case <-time.After(50 * time.Millisecond):
			case <-ctx.Done():
				return
			}

			continue
		}

		for _, msg := range messages {
			t.handleDurableMessage(ctx, js, ex, b, meta, msg)
		}
	}
}

func (t *Transport) handleDurableMessage(
	ctx context.Context, js nats.JetStreamContext, ex action.Executable,
	b DurableBinding, meta *action.Meta, msg *nats.Msg,
) {
	reqCtx, scope, release := xctx.NewScope(ctx)
	defer release()

	scope.Endpoint = "nats.durable." + msg.Subject
	reqCtx = extractContextHeaders(reqCtx, msg, scope)

	decoder := func(v any) error {
		if len(msg.Data) == 0 {
			return nil
		}

		return t.codec.Unmarshal(msg.Data, v)
	}

	var execErr error
	if meta != nil && meta.Idempotency.Enabled {
		_, execErr = executeWithIdempotency(reqCtx, t.idemStore, meta.Idempotency, ex, msg, decoder, t.codec)
	} else {
		_, execErr = ex.ExecuteDecoded(reqCtx, decoder)
	}

	if execErr == nil {
		ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultShutdownTimeout)
		defer cancel()

		_ = msg.AckSync(nats.Context(ackCtx))

		return
	}

	metaInfo, metadataErr := msg.Metadata()
	if metadataErr != nil || b.MaxDeliver <= 0 || metaInfo.NumDelivered < uint64(b.MaxDeliver) {
		delay := b.RetryDelay
		if retry, ok := execErr.(interface{ RetryAfter() time.Duration }); ok && retry.RetryAfter() > 0 {
			delay = retry.RetryAfter()
		}

		_ = msg.NakWithDelay(delay)

		return
	}

	headers := make(map[string]string, len(msg.Header))
	for key, values := range msg.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}

	dead := DeadLetter{
		Stream:     b.Stream,
		Subject:    msg.Subject,
		Durable:    b.Durable,
		Deliveries: metaInfo.NumDelivered,
		FailedAt:   time.Now().UTC(),
		Error:      execErr.Error(),
		Payload:    append([]byte(nil), msg.Data...),
		Headers:    headers,
	}

	payload, marshalErr := json.Marshal(dead)
	if marshalErr != nil {
		_ = msg.NakWithDelay(b.RetryDelay)

		return
	}

	dlqMsg := nats.NewMsg(b.DeadLetterSubject)

	dlqMsg.Data = payload
	if _, publishErr := js.PublishMsg(dlqMsg); publishErr != nil {
		_ = msg.NakWithDelay(b.RetryDelay)

		return
	}

	_ = msg.Term()
}

func validateDurableBinding(b DurableBinding) error {
	if b.Stream == "" || b.Subject == "" || b.Durable == "" || b.DeadLetterSubject == "" {
		return xerr.BadRequest("durable work requires stream, subject, durable consumer, and dead-letter subject")
	}

	if b.MaxDeliver <= 0 || b.AckWait <= 0 || b.RetryDelay <= 0 || b.MaxAckPending <= 0 {
		return xerr.BadRequest("durable delivery policy parameters must be positive")
	}

	return nil
}

func (t *Transport) ensureJetStream() (nats.JetStreamContext, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.js != nil {
		return t.js, nil
	}

	if t.conn == nil {
		return nil, xerr.Unavailable("nats: connection not ready")
	}

	js, err := t.conn.JetStream()
	if err != nil {
		return nil, MapError(err)
	}

	t.js = js

	return js, nil
}

func (t *Transport) ensureDurableInfra(js nats.JetStreamContext, b DurableBinding) error {
	if err := t.ensureStream(js, b.Stream, b.Subject); err != nil {
		return err
	}

	return t.ensureStream(js, b.Stream+"_DLQ", b.DeadLetterSubject)
}

// ensureStream tworzy strumień z podaną listą tematów lub rozszerza istniejący o brakujące tematy.
func (t *Transport) ensureStream(js nats.JetStreamContext, name string, subjects ...string) error {
	if len(subjects) == 0 {
		return nil
	}

	info, err := js.StreamInfo(name)
	if err != nil && !errors.Is(err, nats.ErrStreamNotFound) {
		return MapError(err)
	}

	if errors.Is(err, nats.ErrStreamNotFound) {
		cfg := &nats.StreamConfig{
			Name:      name,
			Subjects:  subjects,
			Storage:   nats.FileStorage,
			Retention: nats.LimitsPolicy,
			Discard:   nats.DiscardOld,
			MaxAge:    7 * 24 * time.Hour,
		}
		if t.streamConfigModifier != nil {
			t.streamConfigModifier(cfg)
		}

		_, addErr := js.AddStream(cfg)
		if addErr != nil && !errors.Is(addErr, nats.ErrStreamNameAlreadyInUse) {
			return MapError(addErr)
		}

		return nil
	}

	updated := false

	for _, s := range subjects {
		found := false

		for _, cs := range info.Config.Subjects {
			if cs == s || subjectPatternCovers(cs, s) {
				found = true

				break
			}
		}

		if !found {
			info.Config.Subjects = append(info.Config.Subjects, s)
			updated = true
		}
	}

	if updated {
		_, updateErr := js.UpdateStream(&info.Config)

		return MapError(updateErr)
	}

	return nil
}

func (t *Transport) ValidateProduction() error {
	t.mu.RLock()
	u := t.url
	t.mu.RUnlock()

	if u == "" {
		return errors.New("NATS URL is required in production")
	}

	if !strings.HasPrefix(u, "tls://") && !strings.HasPrefix(u, "nats://") {
		return errors.New("NATS URL must use nats:// or tls:// scheme")
	}

	if t.productionSecurity == nil && t.productionValidator == nil {
		return errors.New(
			"NATS production security is required; configure " +
				"tnats.WithProductionSecurity or tnats.WithProductionValidation",
		)
	}

	if t.productionSecurity == nil {
		return t.productionValidator()
	}

	if t.productionSecurity.CredentialsFile == "" {
		return errors.New("NATS credentials file is required in production")
	}

	if t.productionSecurity.RootCAFile == "" {
		return errors.New("NATS root CA file is required in production")
	}

	if err := validateFile("credentials", t.productionSecurity.CredentialsFile); err != nil {
		return err
	}

	if err := validateFile("root CA", t.productionSecurity.RootCAFile); err != nil {
		return err
	}

	if t.productionSecurity.ClientCertFile != "" || t.productionSecurity.ClientKeyFile != "" {
		if err := validateFile("client cert", t.productionSecurity.ClientCertFile); err != nil {
			return err
		}

		if err := validateFile("client key", t.productionSecurity.ClientKeyFile); err != nil {
			return err
		}
	}

	rootPEM, err := os.ReadFile(t.productionSecurity.RootCAFile)
	if err != nil {
		return fmt.Errorf("read NATS root CA file: %w", err)
	}

	if !x509.NewCertPool().AppendCertsFromPEM(rootPEM) {
		return fmt.Errorf("parse NATS root CA file %q", t.productionSecurity.RootCAFile)
	}

	if t.productionValidator != nil {
		return t.productionValidator()
	}

	return nil
}

func validateFile(label, path string) error {
	if path == "" {
		return fmt.Errorf("NATS %s path is required", label)
	}

	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("NATS %s file: %w", label, err)
	}

	if info.IsDir() {
		return fmt.Errorf("NATS %s file %q is a directory", label, path)
	}

	return nil
}

func sanitizeURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}

	return u.Redacted()
}

func injectContextHeaders(ctx context.Context, msg *nats.Msg) {
	reqID := xctx.RequestIDFrom(ctx)
	execID := action.ExecutionIDFrom(ctx)
	traceID := action.TraceIDFrom(ctx)
	spanID := action.SpanIDFrom(ctx)

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
		ctx = action.WithExecutionID(ctx, execID)
	}

	traceID := msg.Header.Get(transport.HeaderTraceID)

	spanID := msg.Header.Get(transport.HeaderSpanID)
	if traceID != "" || spanID != "" {
		ctx = action.WithTraceContext(ctx, traceID, spanID)
	}

	return ctx
}

// subjectPatternCovers reports whether an existing stream subject pattern
// already accepts a concrete subject, avoiding invalid wildcard overlaps.
func subjectPatternCovers(pattern, subject string) bool {
	if pattern == subject {
		return true
	}

	patternStart, subjectStart := 0, 0
	for {
		patternEnd := patternStart
		for patternEnd < len(pattern) && pattern[patternEnd] != '.' {
			patternEnd++
		}

		subjectEnd := subjectStart
		for subjectEnd < len(subject) && subject[subjectEnd] != '.' {
			subjectEnd++
		}

		patternToken := pattern[patternStart:patternEnd]
		if patternToken == ">" {
			return true
		}

		if subjectStart >= len(subject) || (patternToken != "*" && patternToken != subject[subjectStart:subjectEnd]) {
			return false
		}

		if patternEnd == len(pattern) || subjectEnd == len(subject) {
			return patternEnd == len(pattern) && subjectEnd == len(subject)
		}

		patternStart = patternEnd + 1
		subjectStart = subjectEnd + 1
	}
}
