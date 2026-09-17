package tnats

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
)

type DeliverPolicy uint8

const (
	DeliverAll DeliverPolicy = iota
	DeliverLast
	DeliverNew
	DeliverByStartSequence
	DeliverByStartTime
	DeliverLastPerSubject
)

type AckPolicy uint8

const (
	AckExplicit AckPolicy = iota
	AckNone
	AckAll
)

type ReplayPolicy uint8

const (
	ReplayInstant ReplayPolicy = iota
	ReplayOriginal
)

type ConsumerBinding struct {
	Stream            string
	Subject           string
	FilterSubjects    []string
	Durable           string
	DeliverPolicy     DeliverPolicy
	OptStartSeq       uint64
	OptStartTime      time.Time
	AckPolicy         AckPolicy
	ReplayPolicy      ReplayPolicy
	MaxDeliver        int
	AckWait           time.Duration
	MaxAckPending     int
	BackOff           []time.Duration
	RateLimitBits     uint64
	HeadersOnly       bool
	DeadLetterSubject string
}

func (b ConsumerBinding) String() string {
	return "nats consumer: " + b.Stream + "/" + b.Subject + " [" + b.Durable + "]"
}

func Consumer(stream, subject, durable string) ConsumerBinding {
	return ConsumerBinding{
		Stream:        stream,
		Subject:       subject,
		Durable:       durable,
		DeliverPolicy: DeliverAll,
		AckPolicy:     AckExplicit,
		ReplayPolicy:  ReplayInstant,
		MaxDeliver:    5,
		AckWait:       30 * time.Second,
		MaxAckPending: 256,
	}
}

func (b ConsumerBinding) FromSequence(seq uint64) ConsumerBinding {
	b.DeliverPolicy = DeliverByStartSequence
	b.OptStartSeq = seq

	return b
}

func (b ConsumerBinding) FromTime(t time.Time) ConsumerBinding {
	b.DeliverPolicy = DeliverByStartTime
	b.OptStartTime = t

	return b
}

func (b ConsumerBinding) NewOnly() ConsumerBinding {
	b.DeliverPolicy = DeliverNew

	return b
}

func (b ConsumerBinding) LastPerSubject() ConsumerBinding {
	b.DeliverPolicy = DeliverLastPerSubject

	return b
}

func (b ConsumerBinding) WithAckPolicy(p AckPolicy) ConsumerBinding {
	b.AckPolicy = p

	return b
}

func (b ConsumerBinding) WithFilters(subjects ...string) ConsumerBinding {
	b.FilterSubjects = append([]string(nil), subjects...)

	return b
}

func (b ConsumerBinding) WithBackOff(backoff ...time.Duration) ConsumerBinding {
	b.BackOff = append([]time.Duration(nil), backoff...)

	return b
}

func (b ConsumerBinding) WithRateLimit(bitsPerSecond uint64) ConsumerBinding {
	b.RateLimitBits = bitsPerSecond

	return b
}

func (b ConsumerBinding) WithHeadersOnly() ConsumerBinding {
	b.HeadersOnly = true

	return b
}

func (b ConsumerBinding) ToDeadLetter(subject string) ConsumerBinding {
	b.DeadLetterSubject = subject

	return b
}

func validateConsumerBinding(b ConsumerBinding) error {
	if b.Stream == "" || b.Subject == "" || b.Durable == "" {
		return xerr.BadRequest("consumer requires stream, subject and durable")
	}

	if b.MaxDeliver <= 0 || b.AckWait <= 0 || b.MaxAckPending <= 0 {
		return xerr.BadRequest("consumer delivery parameters must be positive")
	}

	if len(b.BackOff) > 0 && len(b.BackOff) >= b.MaxDeliver {
		return xerr.BadRequest("consumer backoff length must be < MaxDeliver")
	}

	return nil
}

func (t *Transport) subscribeConsumer(
	ctx context.Context, nc *nats.Conn, js nats.JetStreamContext,
	ex action.Executable, b ConsumerBinding, meta *action.Meta,
) error {
	if err := validateConsumerBinding(b); err != nil {
		return err
	}

	if err := t.ensureConsumerInfra(js, b); err != nil {
		return err
	}

	sub, err := t.openConsumerSubscription(js, b)
	if err != nil {
		return err
	}

	t.mu.Lock()
	t.subs = append(t.subs, sub)
	t.mu.Unlock()

	t.workersWg.Add(1)
	go t.runFetchLoop(ctx, nc, sub, func(msg *nats.Msg) {
		t.handleConsumerMessage(ctx, js, ex, b, meta, msg)
	})

	return nil
}

// runFetchLoop is the shared pull-consumer pump used by both Durable and
// Consumer bindings. It fetches batches, handles timeouts/reconnects, and
// dispatches each message to handle.
func (t *Transport) runFetchLoop(
	ctx context.Context, nc *nats.Conn, sub *nats.Subscription, handle func(*nats.Msg),
) {
	defer t.workersWg.Done()
	defer func() { _ = sub.Unsubscribe() }() //nolint:errcheck // unsubscribe on shutdown is best-effort

	for {
		if ctx.Err() != nil {
			return
		}

		fetchCtx, fetchCancel := context.WithTimeout(ctx, t.fetchTimeout)
		msgs, err := sub.Fetch(t.fetchBatchSize, nats.Context(fetchCtx))

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

		for _, msg := range msgs {
			handle(msg)
		}
	}
}

func (t *Transport) ensureConsumerInfra(js nats.JetStreamContext, b ConsumerBinding) error {
	subjects := []string{b.Subject}
	if len(b.FilterSubjects) > 0 {
		subjects = b.FilterSubjects
	}
	// Register all subjects on the stream so NATS accepts the consumer filter.
	if err := t.ensureStream(js, b.Stream, subjects...); err != nil {
		return err
	}

	if b.DeadLetterSubject != "" {
		if err := t.ensureStream(js, b.Stream+"_DLQ", b.DeadLetterSubject); err != nil {
			return err
		}
	}

	return nil
}

func applyAckPolicyOpts(opts []nats.SubOpt, p AckPolicy) []nats.SubOpt {
	switch p {
	case AckNone:
		return append(opts, nats.AckNone())
	case AckAll:
		return append(opts, nats.AckAll())
	case AckExplicit:
		return append(opts, nats.AckExplicit())
	}

	return append(opts, nats.AckExplicit())
}

func applyDeliverPolicyOpts(opts []nats.SubOpt, b ConsumerBinding) []nats.SubOpt {
	switch b.DeliverPolicy {
	case DeliverLast:
		return append(opts, nats.DeliverLast())
	case DeliverNew:
		return append(opts, nats.DeliverNew())
	case DeliverByStartSequence:
		return append(opts, nats.StartSequence(b.OptStartSeq))
	case DeliverByStartTime:
		return append(opts, nats.StartTime(b.OptStartTime))
	case DeliverLastPerSubject:
		return append(opts, nats.DeliverLastPerSubject())
	case DeliverAll:
		return append(opts, nats.DeliverAll())
	}

	return append(opts, nats.DeliverAll())
}

func applyReplayPolicyOpts(opts []nats.SubOpt, p ReplayPolicy) []nats.SubOpt {
	switch p {
	case ReplayOriginal:
		return append(opts, nats.ReplayOriginal())
	case ReplayInstant:
		return append(opts, nats.ReplayInstant())
	}

	return append(opts, nats.ReplayInstant())
}

func (t *Transport) openConsumerSubscription(js nats.JetStreamContext, b ConsumerBinding) (*nats.Subscription, error) {
	opts := []nats.SubOpt{
		nats.BindStream(b.Stream),
		nats.AckWait(b.AckWait),
		nats.MaxDeliver(b.MaxDeliver),
		nats.MaxAckPending(b.MaxAckPending),
	}

	if len(b.FilterSubjects) > 0 {
		opts = append(opts, nats.ConsumerFilterSubjects(b.FilterSubjects...))
	}

	opts = applyAckPolicyOpts(opts, b.AckPolicy)
	opts = applyDeliverPolicyOpts(opts, b)
	opts = applyReplayPolicyOpts(opts, b.ReplayPolicy)

	if len(b.BackOff) > 0 {
		opts = append(opts, nats.BackOff(b.BackOff))
	}

	if b.RateLimitBits > 0 {
		opts = append(opts, nats.RateLimit(b.RateLimitBits))
	}

	if b.HeadersOnly {
		opts = append(opts, nats.HeadersOnly())
	}

	subject := b.Subject
	if len(b.FilterSubjects) > 0 {
		subject = ""
	}

	sub, err := js.PullSubscribe(subject, b.Durable, opts...)
	if err != nil {
		return nil, MapError(err)
	}

	return sub, nil
}

func (t *Transport) handleConsumerMessage(
	ctx context.Context, js nats.JetStreamContext, ex action.Executable,
	b ConsumerBinding, meta *action.Meta, msg *nats.Msg,
) {
	reqCtx, scope, release := xctx.NewScope(ctx)
	defer release()

	scope.Endpoint = "nats.consumer." + msg.Subject
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
		if b.AckPolicy == AckNone {
			return
		}

		ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultShutdownTimeout)
		defer cancel()

		_ = msg.AckSync(nats.Context(ackCtx)) //nolint:errcheck // ack failure is not recoverable by the handler

		return
	}

	metaInfo, mErr := msg.Metadata()
	if mErr == nil && b.MaxDeliver > 0 && metaInfo.NumDelivered >= uint64(b.MaxDeliver) {
		if b.DeadLetterSubject != "" {
			t.dlqConsumerMessage(js, b, msg, execErr, metaInfo.NumDelivered)
		} else {
			_ = msg.Term() //nolint:errcheck // terminal state; no retry possible
		}

		return
	}

	// When BackOff is defined, let the JetStream engine drive retry intervals.
	if len(b.BackOff) > 0 {
		_ = msg.Nak() //nolint:errcheck // NAck failure falls back to AckWait redelivery

		return
	}

	delay := time.Second
	if retryable, ok := execErr.(interface{ RetryAfter() time.Duration }); ok && retryable.RetryAfter() > 0 {
		delay = retryable.RetryAfter()
	}

	_ = msg.NakWithDelay(delay) //nolint:errcheck // NAck failure falls back to AckWait redelivery
}

func (t *Transport) dlqConsumerMessage(
	js nats.JetStreamContext, b ConsumerBinding, msg *nats.Msg,
	execErr error, deliveries uint64,
) {
	headers := make(map[string]string, len(msg.Header))
	for k, vv := range msg.Header {
		if len(vv) > 0 {
			headers[k] = vv[0]
		}
	}

	dead := DeadLetter{
		Stream:     b.Stream,
		Subject:    msg.Subject,
		Durable:    b.Durable,
		Deliveries: deliveries,
		FailedAt:   time.Now().UTC(),
		Error:      execErr.Error(),
		Payload:    append([]byte(nil), msg.Data...),
		Headers:    headers,
	}

	payload, err := json.Marshal(dead)
	if err != nil {
		_ = msg.Nak() //nolint:errcheck // NAck failure falls back to AckWait redelivery

		return
	}

	dlqMsg := nats.NewMsg(b.DeadLetterSubject)

	dlqMsg.Data = payload
	if _, err := js.PublishMsg(dlqMsg); err != nil {
		_ = msg.Nak() //nolint:errcheck // NAck failure falls back to AckWait redelivery

		return
	}

	_ = msg.Term() //nolint:errcheck // terminal state; no retry possible
}

var _ action.Binding = ConsumerBinding{}
