package tnats

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/xerr"
)

func (t *Transport) Publish(ctx context.Context, subject string, payload any) error {
	nc, err := t.EnsureConn(ctx)
	if err != nil {
		return err
	}

	body, err := t.codec.Marshal(payload)
	if err != nil {
		return xerr.Internal("nats: marshal payload", err)
	}

	msg := &nats.Msg{Subject: subject, Data: body}
	propagateToMsg(ctx, msg)

	return nc.PublishMsg(msg)
}

func (t *Transport) PublishDurable(ctx context.Context, binding DurableBinding, payload any) error {
	if err := validateDurableBinding(binding); err != nil {
		return err
	}

	if _, err := t.EnsureConn(ctx); err != nil {
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

func (t *Transport) Request(ctx context.Context, subject string, payload, resPtr any) error {
	nc, err := t.EnsureConn(ctx)
	if err != nil {
		return err
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
		return xerr.Internal("remote nats action error: " + string(resMsg.Data))
	}

	if resPtr != nil && len(resMsg.Data) > 0 {
		if unmarshalErr := t.codec.Unmarshal(resMsg.Data, resPtr); unmarshalErr != nil {
			return xerr.Internal("failed to unmarshal nats response", unmarshalErr)
		}
	}

	return nil
}
