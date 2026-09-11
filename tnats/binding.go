package tnats

import (
	"fmt"
	"time"

	"github.com/nexssp/kernel/action"
)

// TopicBinding binduje asynchroniczny strumień zdarzeń Pub/Sub do akcji.
type TopicBinding struct {
	Subject    string
	QueueGroup string
}

func (b TopicBinding) String() string {
	if b.QueueGroup != "" {
		return fmt.Sprintf("nats pubsub: %s (queue: %s)", b.Subject, b.QueueGroup)
	}

	return "nats pubsub: " + b.Subject
}

func Topic(subject string, queueGroup ...string) TopicBinding {
	b := TopicBinding{Subject: subject}
	if len(queueGroup) > 0 {
		b.QueueGroup = queueGroup[0]
	}

	return b
}

func Listen(subject string, queueGroup ...string) TopicBinding {
	return Topic(subject, queueGroup...)
}

// RequestBinding binduje synchroniczne punkty końcowe RPC.
type RequestBinding struct {
	Subject string
	Timeout time.Duration
}

func (b RequestBinding) String() string {
	return "nats rpc: " + b.Subject
}

func Request(subject string, timeout ...time.Duration) RequestBinding {
	rb := RequestBinding{Subject: subject}
	if len(timeout) > 0 {
		rb.Timeout = timeout[0]
	}

	return rb
}

// KVBinding binduje nasłuchiwanie zmian w buckecie JetStream Key-Value.
type KVBinding struct {
	Bucket string
	Key    string
}

func (b KVBinding) String() string {
	return fmt.Sprintf("nats kv: %s/%s", b.Bucket, b.Key)
}

func KV(bucket, key string) KVBinding {
	return KVBinding{Bucket: bucket, Key: key}
}

func WatchKV(bucket, key string) KVBinding {
	return KV(bucket, key)
}

// DurableBinding binduje kolejki pracownicze JetStream ze wsparciem dla DLQ.
type DurableBinding struct {
	Stream            string
	Subject           string
	Durable           string
	DeadLetterSubject string
	MaxDeliver        int
	AckWait           time.Duration
	RetryDelay        time.Duration
	MaxAckPending     int
}

func (b DurableBinding) String() string {
	return fmt.Sprintf("nats durable work: %s (stream: %s, durable: %s)", b.Subject, b.Stream, b.Durable)
}

func DurableWork(stream, subject, durable, deadLetterSubject string) DurableBinding {
	return DurableBinding{
		Stream:            stream,
		Subject:           subject,
		Durable:           durable,
		DeadLetterSubject: deadLetterSubject,
		MaxDeliver:        5,
		AckWait:           30 * time.Second,
		RetryDelay:        time.Second,
		MaxAckPending:     64,
	}
}

func (b DurableBinding) WithDeliveryPolicy(
	maxDeliver int, ackWait, retryDelay time.Duration, maxAckPending int,
) DurableBinding {
	if maxDeliver > 0 {
		b.MaxDeliver = maxDeliver
	}

	if ackWait > 0 {
		b.AckWait = ackWait
	}

	if retryDelay > 0 {
		b.RetryDelay = retryDelay
	}

	if maxAckPending > 0 {
		b.MaxAckPending = maxAckPending
	}

	return b
}

var (
	_ action.Binding = TopicBinding{}
	_ action.Binding = RequestBinding{}
	_ action.Binding = KVBinding{}
	_ action.Binding = DurableBinding{}
)
