package tnats

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/xerr"
)

// HeaderErrorMarker oznacza zdalną odpowiedź NATS zawierającą błąd aplikacji.
const HeaderErrorMarker = "X-NATS-Error"

// MapError konwertuje specyficzne kody błędów klienta i brokera NATS na taksonomię xerr.
func MapError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, nats.ErrNoServers) || errors.Is(err, nats.ErrConnectionClosed) {
		return xerr.Unavailable("nats: connection unavailable", err)
	}

	if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
		return xerr.Timeout("nats: operation timed out", err)
	}

	if errors.Is(err, nats.ErrAuthorization) {
		return xerr.Unauthorized("nats: authorization failed", err)
	}

	if errors.Is(err, nats.ErrStreamNotFound) {
		return xerr.NotFound("nats: jetstream stream not found", err)
	}

	if errors.Is(err, nats.ErrBucketNotFound) {
		return xerr.NotFound("nats: key-value bucket not found", err)
	}

	if errors.Is(err, nats.ErrKeyNotFound) {
		return xerr.NotFound("nats: key not found in bucket", err)
	}

	var appErr *xerr.AppError
	if errors.As(err, &appErr) {
		return appErr
	}

	return xerr.Internal("nats: transport error", err)
}
