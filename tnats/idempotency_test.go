package tnats_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/transportnats/tnats"
)

type PaymentReq struct {
	ID     string  `json:"id"`
	Amount float64 `json:"amount"`
}

type PaymentRes struct {
	TxID   string `json:"tx_id"`
	Status string `json:"status"`
}

func TestIdempotency_HappyPath_Deduplication(t *testing.T) {
	t.Parallel()
	_, natsURL := runEmbeddedNATS(t)

	var executions atomic.Int32
	payAct := action.New("payment.charge", func(ctx context.Context, req PaymentReq) (PaymentRes, error) {
		executions.Add(1)

		return PaymentRes{TxID: "tx_" + req.ID, Status: "CONFIRMED"}, nil
	}).
		Idempotent().
		Route(tnats.Request("payments.charge", 2*time.Second)).
		Build()

	tr := tnats.New(natsURL)
	tr.Mount([]action.AnyAction{payAct})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	nc := tr.Conn()
	reqData, _ := json.Marshal(PaymentReq{ID: "pay_100", Amount: 50.0})

	msg1 := nats.NewMsg("payments.charge")
	msg1.Data = reqData
	msg1.Header.Set("Idempotency-Key", "IDEM-KEY-001")

	res1, err := nc.RequestMsgWithContext(ctx, msg1)
	if err != nil {
		t.Fatalf("first request failed: %v", err)
	}

	msg2 := nats.NewMsg("payments.charge")
	msg2.Data = reqData
	msg2.Header.Set("Idempotency-Key", "IDEM-KEY-001")

	res2, err := nc.RequestMsgWithContext(ctx, msg2)
	if err != nil {
		t.Fatalf("second duplicate request failed: %v", err)
	}

	if string(res1.Data) != string(res2.Data) {
		t.Fatalf("cached response mismatch: %s != %s", string(res1.Data), string(res2.Data))
	}

	if runs := executions.Load(); runs != 1 {
		t.Fatalf("expected exactly 1 action run, got %d", runs)
	}
}

func TestIdempotency_BadPath_PayloadConflict(t *testing.T) {
	t.Parallel()
	_, natsURL := runEmbeddedNATS(t)

	var executions atomic.Int32
	payAct := action.New("payment.conflict", func(ctx context.Context, req PaymentReq) (PaymentRes, error) {
		executions.Add(1)

		return PaymentRes{TxID: "tx_" + req.ID, Status: "CONFIRMED"}, nil
	}).
		Idempotent().
		Route(tnats.Request("payments.conflict", 2*time.Second)).
		Build()

	tr := tnats.New(natsURL)
	tr.Mount([]action.AnyAction{payAct})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	nc := tr.Conn()

	data1, _ := json.Marshal(PaymentReq{ID: "pay_1", Amount: 10.0})
	msg1 := nats.NewMsg("payments.conflict")
	msg1.Data = data1
	msg1.Header.Set("Idempotency-Key", "SHARED-KEY-XYZ")

	if _, err := nc.RequestMsgWithContext(ctx, msg1); err != nil {
		t.Fatalf("first request failed: %v", err)
	}

	data2, _ := json.Marshal(PaymentReq{ID: "pay_1", Amount: 999.0})
	msg2 := nats.NewMsg("payments.conflict")
	msg2.Data = data2
	msg2.Header.Set("Idempotency-Key", "SHARED-KEY-XYZ")

	res2, err := nc.RequestMsgWithContext(ctx, msg2)
	if err != nil {
		t.Fatalf("expected NATS reply message, got transport err: %v", err)
	}

	if res2.Header.Get(tnats.HeaderErrorMarker) != "1" {
		t.Fatalf("expected X-NATS-Error marker on payload conflict, got none")
	}

	var errResp xerr.ErrorResponse
	_ = json.Unmarshal(res2.Data, &errResp)
	if errResp.Error != string(xerr.KindConflict) {
		t.Fatalf("expected Conflict error kind, got %s", errResp.Error)
	}

	if executions.Load() != 1 {
		t.Fatalf("action should not execute for conflicting payload, executions=%d", executions.Load())
	}
}

func TestIdempotency_BadPath_ConcurrentInProgress(t *testing.T) {
	t.Parallel()
	_, natsURL := runEmbeddedNATS(t)

	started := make(chan struct{})
	block := make(chan struct{})

	slowAct := action.New("payment.slow", func(ctx context.Context, req PaymentReq) (PaymentRes, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-block

		return PaymentRes{TxID: "tx_slow", Status: "DONE"}, nil
	}).
		Idempotent().
		Route(tnats.Request("payments.slow", 3*time.Second)).
		Build()

	tr := tnats.New(natsURL)
	tr.Mount([]action.AnyAction{slowAct})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	nc := tr.Conn()
	data, _ := json.Marshal(PaymentReq{ID: "pay_slow", Amount: 100})

	go func() {
		msg1 := nats.NewMsg("payments.slow")
		msg1.Data = data
		msg1.Header.Set("Idempotency-Key", "CONCURRENT-KEY-1")
		_, _ = nc.RequestMsgWithContext(ctx, msg1)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first request did not start")
	}

	msg2 := nats.NewMsg("payments.slow")
	msg2.Data = data
	msg2.Header.Set("Idempotency-Key", "CONCURRENT-KEY-1")

	res2, err := nc.RequestMsgWithContext(ctx, msg2)
	close(block)

	if err != nil {
		t.Fatalf("request failed: %v", err)
	}

	if res2.Header.Get(tnats.HeaderErrorMarker) != "1" {
		t.Fatalf("expected X-NATS-Error for in-progress request")
	}

	var errResp xerr.ErrorResponse
	_ = json.Unmarshal(res2.Data, &errResp)
	if errResp.Error != string(xerr.KindUnavailable) {
		t.Fatalf("expected Unavailable error kind, got %s", errResp.Error)
	}
}

func TestKVIdempotencyCoordinator_Lifecycle(t *testing.T) {
	t.Parallel()
	_, natsURL := runEmbeddedNATS(t)

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	coord, err := tnats.NewKVIdempotencyCoordinator(js, "TEST_IDEM_LIFECYCLE")
	if err != nil {
		t.Fatalf("NewKVIdempotencyCoordinator: %v", err)
	}

	ctx := context.Background()
	key := "IDEM_LIFECYCLE_KEY"
	reqHash := "hash_version_1"

	claim1, err := coord.Claim(ctx, key, reqHash, 100*time.Millisecond)
	if err != nil || claim1.State != action.IdempotencyClaimAcquired {
		t.Fatalf("expected Acquired, got state=%v, err=%v", claim1.State, err)
	}

	claim2, err := coord.Claim(ctx, key, reqHash, 100*time.Millisecond)
	if err != nil || claim2.State != action.IdempotencyClaimInProgress {
		t.Fatalf("expected InProgress, got state=%v, err=%v", claim2.State, err)
	}

	_ = coord.Release(ctx, key, "fake-token")
	claimAfterFake, _ := coord.Claim(ctx, key, reqHash, 100*time.Millisecond)
	if claimAfterFake.State != action.IdempotencyClaimInProgress {
		t.Fatalf("fake token must not release lease")
	}

	time.Sleep(150 * time.Millisecond)

	claim3, err := coord.Claim(ctx, key, reqHash, 2*time.Second)
	if err != nil || claim3.State != action.IdempotencyClaimAcquired {
		t.Fatalf("expired lease must be takeable by new claim, got state=%v", claim3.State)
	}

	entry := action.IdempotencyEntry{
		Status:      200,
		Body:        []byte("SUCCESS_PAYLOAD"),
		RequestHash: reqHash,
	}
	if err := coord.Complete(ctx, key, claim3.Token, entry, time.Hour); err != nil {
		t.Fatalf("Complete failed: %v", err)
	}

	claimDone, err := coord.Claim(ctx, key, reqHash, time.Second)
	if err != nil || claimDone.State != action.IdempotencyClaimCompleted {
		t.Fatalf("expected Completed state, got %v", claimDone.State)
	}
	if string(claimDone.Entry.Body) != "SUCCESS_PAYLOAD" {
		t.Fatalf("unexpected cached body: %s", string(claimDone.Entry.Body))
	}
}

func TestKVIdempotencyCoordinator_EntryTTLAndOwnership(t *testing.T) {
	t.Parallel()
	_, natsURL := runEmbeddedNATS(t)
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	coord, err := tnats.NewKVIdempotencyCoordinator(js, "TEST_IDEM_TTL")
	if err != nil {
		t.Fatalf("coordinator: %v", err)
	}
	ctx := context.Background()
	claim, err := coord.Claim(ctx, "ttl-key", "hash", time.Second)
	if err != nil || claim.State != action.IdempotencyClaimAcquired {
		t.Fatalf("claim: %+v, %v", claim, err)
	}
	entry := action.IdempotencyEntry{Status: 200, Body: []byte("body"), RequestHash: "hash"}
	if err := coord.Complete(ctx, "ttl-key", "wrong-token", entry, time.Second); err == nil {
		t.Fatal("expected ownership error")
	}
	if err := coord.Complete(ctx, "ttl-key", claim.Token, entry, 40*time.Millisecond); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if done, ok := coord.Get(ctx, "ttl-key"); !ok || string(done.Body) != "body" {
		t.Fatalf("expected completed entry, got %+v, %v", done, ok)
	}
	time.Sleep(60 * time.Millisecond)
	if _, ok := coord.Get(ctx, "ttl-key"); ok {
		t.Fatal("expired entry was returned")
	}
}
