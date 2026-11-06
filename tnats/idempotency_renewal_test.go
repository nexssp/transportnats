package tnats

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/transport/codec"
)

type renewalTestCoordinator struct {
	renewed       chan struct{}
	renewalFailed chan struct{}
	renewErr      error
	count         atomic.Int32
}

func (c *renewalTestCoordinator) Get(context.Context, string) (action.IdempotencyEntry, bool) {
	return action.IdempotencyEntry{}, false
}

func (*renewalTestCoordinator) Set(context.Context, string, action.IdempotencyEntry, time.Duration) {}

func (*renewalTestCoordinator) Claim(context.Context, string, string, time.Duration) (action.IdempotencyClaim, error) {
	return action.IdempotencyClaim{State: action.IdempotencyClaimAcquired, Token: "test-token"}, nil
}

func (*renewalTestCoordinator) Complete(context.Context, string, string, action.IdempotencyEntry, time.Duration) error {
	return nil
}

func (*renewalTestCoordinator) Release(context.Context, string, string) error { return nil }

func (c *renewalTestCoordinator) RenewClaim(ctx context.Context, _, _ string, _ time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.count.Add(1)
	if c.renewErr != nil {
		if c.renewalFailed != nil {
			close(c.renewalFailed)
		}
		return c.renewErr
	}
	select {
	case c.renewed <- struct{}{}:
	default:
	}
	return nil
}

type renewalTestExecutable struct {
	started  chan struct{}
	release  <-chan struct{}
	canceled chan struct{}
}

func (e renewalTestExecutable) ExecuteDecoded(ctx context.Context, _ action.DecodeFunc) (any, error) {
	close(e.started)
	select {
	case <-e.release:
		return map[string]string{"status": "ok"}, nil
	case <-ctx.Done():
		if e.canceled != nil {
			close(e.canceled)
		}
		return nil, ctx.Err()
	}
}

func TestExecuteWithCoordinatorAbortsWhenLeaseRenewalFails(t *testing.T) {
	coord := &renewalTestCoordinator{
		renewalFailed: make(chan struct{}),
		renewErr:      xerr.Unavailable("lease renewal rejected"),
	}
	started := make(chan struct{})
	canceled := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := executeWithCoordinator(
			t.Context(), coord, action.IdempotencyConfig{LeaseTTL: 30 * time.Millisecond},
			renewalTestExecutable{started: started, canceled: canceled}, "key", "hash",
			func(any) error { return nil }, codec.JSON{},
		)
		result <- err
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("idempotent action did not start")
	}
	select {
	case <-coord.renewalFailed:
	case <-time.After(2 * time.Second):
		t.Fatal("coordinator renewal was not attempted")
	}
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("renewal failure did not cancel the running action")
	}
	select {
	case err := <-result:
		if got := xerr.KindFrom(err); got != xerr.KindUnavailable {
			t.Fatalf("executeWithCoordinator error kind = %v, want unavailable: %v", got, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("idempotent action did not finish after renewal failure")
	}
}

func TestExecuteWithCoordinatorRenewsLongRunningClaim(t *testing.T) {
	coord := &renewalTestCoordinator{renewed: make(chan struct{}, 1)}
	started := make(chan struct{})
	release := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := executeWithCoordinator(
			t.Context(), coord, action.IdempotencyConfig{LeaseTTL: 30 * time.Millisecond},
			renewalTestExecutable{started: started, release: release}, "key", "hash",
			func(any) error { return nil }, codec.JSON{},
		)
		result <- err
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("idempotent action did not start")
	}
	select {
	case <-coord.renewed:
	case <-time.After(2 * time.Second):
		t.Fatal("coordinator lease was not renewed while the action was running")
	}
	close(release)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("executeWithCoordinator: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("idempotent action did not finish after lease renewal")
	}
	if got := coord.count.Load(); got == 0 {
		t.Fatal("renewal count = 0, want at least one renewal")
	}
}
