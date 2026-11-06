package tnats

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nexssp/kernel/xerr"
)

// HeaderErrorMarker marks NATS replies that contain a remote application error.
const HeaderErrorMarker = "X-NATS-Error"

// MapError classifies NATS client and JetStream errors as application errors.
func MapError(err error) error {
	if err == nil {
		return nil
	}
	if appErr, ok := errors.AsType[*xerr.AppError](err); ok {
		return appErr
	}

	switch {
	case errors.Is(err, context.Canceled):
		return xerr.Canceled("nats: operation canceled", err)
	case errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, nats.ErrTimeout), errors.Is(err, nats.ErrDrainTimeout),
		errors.Is(err, nats.ErrAsyncPublishTimeout), errors.Is(err, nats.ErrNoHeartbeat):
		return xerr.Timeout("nats: operation timed out", err)
	case errors.Is(err, nats.ErrNoResponders):
		return xerr.Unavailable("nats: no responder is subscribed to the request subject", err)
	case errors.Is(err, nats.ErrNoStreamResponse), errors.Is(err, nats.ErrFetchDisconnected),
		errors.Is(err, nats.ErrConnectionClosed), errors.Is(err, nats.ErrConnectionDraining),
		errors.Is(err, nats.ErrConnectionReconnecting), errors.Is(err, nats.ErrDisconnected),
		errors.Is(err, nats.ErrStaleConnection), errors.Is(err, nats.ErrNoServers),
		errors.Is(err, nats.ErrReconnectBufExceeded), errors.Is(err, nats.ErrHeadersNotSupported),
		errors.Is(err, nats.ErrConsumerDeleted), errors.Is(err, nats.ErrConsumerLeadershipChanged),
		errors.Is(err, nats.ErrSubscriptionClosed), errors.Is(err, nats.ErrJetStreamPublisherClosed),
		errors.Is(err, nats.ErrConsumerMultipleFilterSubjectsNotSupported),
		errors.Is(err, jetstream.ErrJetStreamNotEnabled),
		errors.Is(err, jetstream.ErrJetStreamNotEnabledForAccount),
		errors.Is(err, jetstream.ErrConsumerMultipleFilterSubjectsNotSupported):
		return xerr.Unavailable("nats: service or required server capability is unavailable", err)
	case errors.Is(err, nats.ErrAuthorization), errors.Is(err, nats.ErrAuthExpired),
		errors.Is(err, nats.ErrAuthRevoked), errors.Is(err, nats.ErrAccountAuthExpired):
		return xerr.Unauthorized("nats: authentication failed", err)
	case errors.Is(err, nats.ErrPermissionViolation):
		return xerr.Forbidden("nats: permission denied", err)
	case errors.Is(err, nats.ErrMaxConnectionsExceeded),
		errors.Is(err, nats.ErrMaxAccountConnectionsExceeded),
		errors.Is(err, nats.ErrMaxSubscriptionsExceeded), errors.Is(err, nats.ErrSlowConsumer),
		errors.Is(err, jetstream.ErrTooManyStalledMsgs):
		return xerr.TooManyRequests("nats: server or client resource limit reached", err)
	case errors.Is(err, nats.ErrStreamNotFound), errors.Is(err, nats.ErrConsumerNotFound),
		errors.Is(err, nats.ErrMsgNotFound), errors.Is(err, nats.ErrNoMatchingStream),
		errors.Is(err, nats.ErrBucketNotFound), errors.Is(err, nats.ErrKeyNotFound),
		errors.Is(err, nats.ErrObjectNotFound), errors.Is(err, jetstream.ErrStreamNotFound),
		errors.Is(err, jetstream.ErrConsumerNotFound), errors.Is(err, jetstream.ErrMsgNotFound),
		errors.Is(err, jetstream.ErrBucketNotFound), errors.Is(err, jetstream.ErrKeyNotFound),
		errors.Is(err, jetstream.ErrKeyDeleted), errors.Is(err, jetstream.ErrNoKeysFound),
		errors.Is(err, jetstream.ErrObjectNotFound), errors.Is(err, jetstream.ErrNoObjectsFound):
		return xerr.NotFound("nats: requested stream, consumer, key, or object was not found", err)
	case errors.Is(err, nats.ErrStreamNameAlreadyInUse), errors.Is(err, nats.ErrConsumerNameAlreadyInUse),
		errors.Is(err, nats.ErrMsgAlreadyAckd), errors.Is(err, nats.ErrKeyExists),
		errors.Is(err, nats.ErrKeyRevisionMismatch), errors.Is(err, jetstream.ErrStreamNameAlreadyInUse),
		errors.Is(err, jetstream.ErrConsumerExists), errors.Is(err, jetstream.ErrKeyExists),
		errors.Is(err, jetstream.ErrKeyRevisionMismatch), errors.Is(err, jetstream.ErrBucketExists):
		return xerr.Conflict("nats: resource already exists or its revision changed", err)
	case errors.Is(err, nats.ErrMaxPayload), errors.Is(err, nats.ErrBadSubject),
		errors.Is(err, nats.ErrBadQueueName), errors.Is(err, nats.ErrInvalidArg),
		errors.Is(err, nats.ErrInvalidMsg), errors.Is(err, nats.ErrBadTimeout),
		errors.Is(err, nats.ErrInvalidContext), errors.Is(err, nats.ErrNoDeadlineContext),
		errors.Is(err, nats.ErrBadHeaderMsg), errors.Is(err, nats.ErrMsgNoReply),
		errors.Is(err, nats.ErrMsgNotBound), errors.Is(err, nats.ErrContextAndTimeout),
		errors.Is(err, nats.ErrInvalidStreamName), errors.Is(err, nats.ErrInvalidConsumerName),
		errors.Is(err, nats.ErrInvalidFilterSubject), errors.Is(err, nats.ErrSubjectMismatch),
		errors.Is(err, nats.ErrStreamConfigRequired), errors.Is(err, nats.ErrStreamNameRequired),
		errors.Is(err, nats.ErrConsumerNameRequired), errors.Is(err, nats.ErrConsumerConfigRequired),
		errors.Is(err, jetstream.ErrInvalidBucketName), errors.Is(err, jetstream.ErrInvalidKey),
		errors.Is(err, jetstream.ErrBadBucket), errors.Is(err, jetstream.ErrBadObjectMeta),
		errors.Is(err, jetstream.ErrInvalidStoreName), errors.Is(err, jetstream.ErrObjectConfigRequired):
		return xerr.BadRequest("nats: invalid request or resource configuration", err)
	}

	if mapped := mapJetStreamAPIError(err); mapped != nil {
		return mapped
	}

	return xerr.Internal("nats: transport error", err)
}

func mapJetStreamAPIError(err error) error {
	if jsErr, ok := errors.AsType[nats.JetStreamError](err); ok {
		if apiErr := jsErr.APIError(); apiErr != nil {
			return mapAPIStatus(err, apiErr.Code)
		}
	}
	if jsErr, ok := errors.AsType[jetstream.JetStreamError](err); ok {
		if apiErr := jsErr.APIError(); apiErr != nil {
			return mapAPIStatus(err, apiErr.Code)
		}
	}

	return nil
}

func mapAPIStatus(err error, status int) error {
	switch status {
	case 400:
		return xerr.BadRequest("nats: JetStream rejected the request", err)
	case 401:
		return xerr.Unauthorized("nats: JetStream authentication failed", err)
	case 403:
		return xerr.Forbidden("nats: JetStream permission denied", err)
	case 404:
		return xerr.NotFound("nats: JetStream resource was not found", err)
	case 408, 504:
		return xerr.Timeout("nats: JetStream request timed out", err)
	case 409:
		return xerr.Conflict("nats: JetStream resource conflict", err)
	case 429:
		return xerr.TooManyRequests("nats: JetStream resource limit reached", err)
	case 503:
		return xerr.Unavailable("nats: JetStream is unavailable", err)
	default:
		return nil
	}
}
