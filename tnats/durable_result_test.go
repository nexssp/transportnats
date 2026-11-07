package tnats_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/action"

	"github.com/nexssp/transportnats/tnats"
)

type durableResultTestEnvelope struct {
	JobID  string          `json:"job_id"`
	Status string          `json:"status"`
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Error     string `json:"error"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
	} `json:"error"`
}

func TestDurablePublishesSuccessfulResultEnvelopeBeforeAcknowledging(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, js := connectJetStream(t, natsURL)
	defer nc.Close()
	addDurableJobStreams(t, js, "WORK_JOBS", "work.jobs", "work.dlq")
	addDurableResultStream(t, js, "WORK_RESULTS", "work.results")
	addDurableResultBucket(t, js, "WORK_RESULTS_KV")
	resultSub, err := js.PullSubscribe("work.results", "result-observer", nats.BindStream("WORK_RESULTS"))
	if err != nil {
		t.Fatalf("subscribe to results: %v", err)
	}
	defer resultSub.Unsubscribe()

	binding := tnats.DurableWork("WORK_JOBS", "work.jobs", "work-worker", "work.dlq").
		WithDeliveryIdentityField("job_id").WithResult("WORK_RESULTS", "work.results")
	worker := action.New("work.success", func(_ context.Context, req map[string]any) (any, error) {
		return map[string]any{"answer": "done", "input": req["value"]}, nil
	}).Route(binding).Build()
	tr := tnats.New(natsURL, tnats.WithKVIdempotencyBucket("WORK_RESULTS_KV"))
	tr.Mount([]action.AnyAction{worker})
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer stopTransport(t, tr, cancel)
	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("wait for worker: %v", err)
	}
	if _, err := js.Publish("work.jobs", []byte(`{"job_id":"job-success-1","value":7}`)); err != nil {
		t.Fatalf("publish job: %v", err)
	}
	msg := fetchOne(t, resultSub)
	var envelope durableResultTestEnvelope
	if err := json.Unmarshal(msg.Data, &envelope); err != nil {
		t.Fatalf("decode result envelope: %v", err)
	}
	if envelope.JobID != "job-success-1" || envelope.Status != "succeeded" || envelope.Error != nil {
		t.Fatalf("result envelope = %#v, want correlated successful result", envelope)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(msg.Data, &wire); err != nil {
		t.Fatalf("decode wire envelope: %v", err)
	}
	if string(wire["error"]) != "null" {
		t.Fatalf("wire error field = %s, want explicit null", wire["error"])
	}
	var output map[string]any
	if err := json.Unmarshal(envelope.Result, &output); err != nil {
		t.Fatalf("decode action output: %v", err)
	}
	if output["answer"] != "done" || output["input"] != float64(7) {
		t.Fatalf("action output = %#v, want action result", output)
	}
}

func TestDurablePublishesFailureResultBeforeDeadLetter(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, js := connectJetStream(t, natsURL)
	defer nc.Close()
	addDurableJobStreams(t, js, "FAIL_WORK", "work.fail", "work.failed.dlq")
	addDurableResultStream(t, js, "FAIL_RESULTS", "fail.results")
	addDurableResultBucket(t, js, "FAIL_RESULTS_KV")
	resultInfo, err := js.StreamInfo("FAIL_RESULTS")
	if err != nil {
		t.Fatalf("inspect result stream: %v", err)
	}
	resultInfo.Config.MaxBytes = 1
	if _, err := js.UpdateStream(&resultInfo.Config); err != nil {
		t.Fatalf("constrain result stream: %v", err)
	}
	resultSub, err := js.PullSubscribe("fail.results", "failure-result-observer", nats.BindStream("FAIL_RESULTS"))
	if err != nil {
		t.Fatalf("subscribe to results: %v", err)
	}
	defer resultSub.Unsubscribe()
	dlqSub, err := js.PullSubscribe("work.failed.dlq", "failure-dlq-observer", nats.BindStream("FAIL_WORK_DLQ"))
	if err != nil {
		t.Fatalf("subscribe to DLQ: %v", err)
	}
	defer dlqSub.Unsubscribe()
	advisorySub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.CONSUMER.MSG_NAKED.>")
	if err != nil {
		t.Fatalf("subscribe to NAK advisories: %v", err)
	}
	defer advisorySub.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush NAK advisory subscription: %v", err)
	}

	binding := tnats.DurableWork("FAIL_WORK", "work.fail", "fail-worker", "work.failed.dlq").
		WithDeliveryPolicy(1, time.Second, 25*time.Millisecond, 8).
		WithDeliveryIdentityField("job_id").WithResult("FAIL_RESULTS", "fail.results")
	var attempts atomic.Int64
	worker := action.New("work.failure", func(_ context.Context, _ map[string]any) (any, error) {
		attempts.Add(1)
		return nil, errors.New("private action failure")
	}).Route(binding).Build()
	tr := tnats.New(natsURL, tnats.WithKVIdempotencyBucket("FAIL_RESULTS_KV"))
	tr.Mount([]action.AnyAction{worker})
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer stopTransport(t, tr, cancel)
	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("wait for worker: %v", err)
	}
	if _, err := js.Publish("work.fail", []byte(`{"job_id":"job-failure-1"}`)); err != nil {
		t.Fatalf("publish job: %v", err)
	}
	if _, err := advisorySub.NextMsg(3 * time.Second); err != nil {
		t.Fatalf("failed result publish did not NAK the source job: %v", err)
	}
	dlqInfo, err := js.StreamInfo("FAIL_WORK_DLQ")
	if err != nil {
		t.Fatalf("inspect DLQ stream: %v", err)
	}
	if dlqInfo.State.Msgs != 0 {
		t.Fatalf("DLQ messages = %d before result publication succeeds, want 0", dlqInfo.State.Msgs)
	}
	resultInfo.Config.MaxBytes = 1 << 20
	if _, err := js.UpdateStream(&resultInfo.Config); err != nil {
		t.Fatalf("restore result stream capacity: %v", err)
	}
	resultMsg := fetchOne(t, resultSub)
	var envelope durableResultTestEnvelope
	if err := json.Unmarshal(resultMsg.Data, &envelope); err != nil {
		t.Fatalf("decode result envelope: %v", err)
	}
	if envelope.JobID != "job-failure-1" || envelope.Status != "failed" || string(envelope.Result) != "null" || envelope.Error == nil {
		t.Fatalf("result envelope = %#v, want failed result with public error", envelope)
	}
	if envelope.Error.RequestID != "job-failure-1" || envelope.Error.Error == "" {
		t.Fatalf("public error = %#v, want safe error with job correlation", envelope.Error)
	}
	if len(resultMsg.Data) == 0 {
		t.Fatal("failure result was empty")
	}
	_ = fetchOne(t, dlqSub)
	if attempts.Load() != 1 {
		t.Fatalf("terminal action attempts = %d, want one execution across result outage", attempts.Load())
	}
}

func TestDurableIdentityConflictIsRejectedBeforeStoredOutcomeReplay(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, js := connectJetStream(t, natsURL)
	defer nc.Close()
	addDurableJobStreams(t, js, "IDENTITY_JOBS", "identity.jobs", "identity.dlq")
	addDurableResultStream(t, js, "IDENTITY_RESULTS", "identity.results")
	addDurableResultBucket(t, js, "IDENTITY_RESULTS_KV")
	resultSub, err := js.PullSubscribe("identity.results", "identity-result-observer", nats.BindStream("IDENTITY_RESULTS"))
	if err != nil {
		t.Fatalf("subscribe to results: %v", err)
	}
	defer resultSub.Unsubscribe()
	dlqSub, err := js.PullSubscribe("identity.dlq", "identity-dlq-observer", nats.BindStream("IDENTITY_JOBS_DLQ"))
	if err != nil {
		t.Fatalf("subscribe to DLQ: %v", err)
	}
	defer dlqSub.Unsubscribe()

	var calls atomic.Int64
	binding := tnats.DurableWork("IDENTITY_JOBS", "identity.jobs", "identity-worker", "identity.dlq").
		WithDeliveryPolicy(3, time.Second, 25*time.Millisecond, 8).
		WithDeliveryIdentityField("job_id").WithResult("IDENTITY_RESULTS", "identity.results")
	worker := action.New("work.identity", func(_ context.Context, _ map[string]any) (any, error) {
		calls.Add(1)
		return map[string]any{"completed": true}, nil
	}).Route(binding).Build()
	tr := tnats.New(natsURL, tnats.WithKVIdempotencyBucket("IDENTITY_RESULTS_KV"))
	tr.Mount([]action.AnyAction{worker})
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer stopTransport(t, tr, cancel)
	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("wait for worker: %v", err)
	}

	jobData := []byte(`{"job_id":"job-conflict-1"}`)
	if _, err := js.Publish("identity.jobs", jobData); err != nil {
		t.Fatalf("publish original job: %v", err)
	}
	result := fetchOne(t, resultSub)
	var envelope durableResultTestEnvelope
	if err := json.Unmarshal(result.Data, &envelope); err != nil {
		t.Fatalf("decode result envelope: %v", err)
	}
	if envelope.JobID != "job-conflict-1" || envelope.Status != "succeeded" {
		t.Fatalf("original result = %#v, want stored success", envelope)
	}
	if err := result.AckSync(); err != nil {
		t.Fatalf("ack original result: %v", err)
	}

	conflict := nats.NewMsg("identity.jobs")
	conflict.Data = jobData
	conflict.Header.Set("Idempotency-Key", "different-job")
	if _, err := js.PublishMsg(conflict); err != nil {
		t.Fatalf("publish identity-conflict job: %v", err)
	}
	var conflictDLQ tnats.DeadLetter
	if err := json.Unmarshal(fetchOne(t, dlqSub).Data, &conflictDLQ); err != nil {
		t.Fatalf("decode conflicting job DLQ record: %v", err)
	}
	if !bytes.Equal(conflictDLQ.Payload, jobData) || conflictDLQ.Headers["Idempotency-Key"] != "different-job" || conflictDLQ.Deliveries != 1 {
		t.Fatalf("conflict DLQ record = %#v, want original payload/header on first delivery", conflictDLQ)
	}

	missingID := []byte(`{"route":"memory"}`)
	if _, err := js.Publish("identity.jobs", missingID); err != nil {
		t.Fatalf("publish job without identity: %v", err)
	}
	var missingDLQ tnats.DeadLetter
	if err := json.Unmarshal(fetchOne(t, dlqSub).Data, &missingDLQ); err != nil {
		t.Fatalf("decode missing-identity DLQ record: %v", err)
	}
	if !bytes.Equal(missingDLQ.Payload, missingID) || missingDLQ.Deliveries != 1 {
		t.Fatalf("missing-identity DLQ record = %#v, want original payload on first delivery", missingDLQ)
	}
	if calls.Load() != 1 {
		t.Fatalf("action calls = %d, want one execution for the original job", calls.Load())
	}
	info, err := js.StreamInfo("IDENTITY_RESULTS")
	if err != nil {
		t.Fatalf("inspect results stream: %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("result message count = %d, want only the original stored result", info.State.Msgs)
	}
}

func TestDurablePayloadConflictIsDeadLetteredWithoutReplay(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, js := connectJetStream(t, natsURL)
	defer nc.Close()
	addDurableJobStreams(t, js, "PAYLOAD_CONFLICT_JOBS", "payload-conflict.jobs", "payload-conflict.dlq")
	addDurableResultStream(t, js, "PAYLOAD_CONFLICT_RESULTS", "payload-conflict.results")
	addDurableResultBucket(t, js, "PAYLOAD_CONFLICT_KV")
	resultSub, err := js.PullSubscribe("payload-conflict.results", "payload-conflict-result-observer", nats.BindStream("PAYLOAD_CONFLICT_RESULTS"))
	if err != nil {
		t.Fatalf("subscribe to results: %v", err)
	}
	defer resultSub.Unsubscribe()
	dlqSub, err := js.PullSubscribe("payload-conflict.dlq", "payload-conflict-dlq-observer", nats.BindStream("PAYLOAD_CONFLICT_JOBS_DLQ"))
	if err != nil {
		t.Fatalf("subscribe to DLQ: %v", err)
	}
	defer dlqSub.Unsubscribe()

	var calls atomic.Int64
	binding := tnats.DurableWork("PAYLOAD_CONFLICT_JOBS", "payload-conflict.jobs", "payload-conflict-worker", "payload-conflict.dlq").
		WithDeliveryPolicy(3, time.Second, 25*time.Millisecond, 8).
		WithDeliveryIdentityField("job_id").WithResult("PAYLOAD_CONFLICT_RESULTS", "payload-conflict.results")
	worker := action.New("work.payload-conflict", func(_ context.Context, req map[string]any) (any, error) {
		calls.Add(1)
		return map[string]any{"value": req["value"]}, nil
	}).Route(binding).Build()
	tr := tnats.New(natsURL, tnats.WithKVIdempotencyBucket("PAYLOAD_CONFLICT_KV"))
	tr.Mount([]action.AnyAction{worker})
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer stopTransport(t, tr, cancel)
	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("wait for worker: %v", err)
	}

	original := []byte(`{"job_id":"same-job-id","value":"original"}`)
	if _, err := js.Publish("payload-conflict.jobs", original); err != nil {
		t.Fatalf("publish original job: %v", err)
	}
	result := fetchOne(t, resultSub)
	var envelope durableResultTestEnvelope
	if err := json.Unmarshal(result.Data, &envelope); err != nil {
		t.Fatalf("decode original result: %v", err)
	}
	if envelope.JobID != "same-job-id" || envelope.Status != "succeeded" {
		t.Fatalf("original result = %#v, want stored success", envelope)
	}
	if err := result.AckSync(); err != nil {
		t.Fatalf("ack original result: %v", err)
	}

	changed := []byte(`{"job_id":"same-job-id","value":"changed"}`)
	if _, err := js.Publish("payload-conflict.jobs", changed); err != nil {
		t.Fatalf("publish conflicting payload: %v", err)
	}
	var dead tnats.DeadLetter
	if err := json.Unmarshal(fetchOne(t, dlqSub).Data, &dead); err != nil {
		t.Fatalf("decode conflicting job DLQ record: %v", err)
	}
	if !bytes.Equal(dead.Payload, changed) || !strings.Contains(dead.Error, "different payload") {
		t.Fatalf("conflict DLQ record = %#v, want changed payload and conflict error", dead)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("action calls = %d, want only the original job execution", got)
	}
	info, err := js.StreamInfo("PAYLOAD_CONFLICT_RESULTS")
	if err != nil {
		t.Fatalf("inspect result stream: %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("result message count = %d, want no replay for conflicting payload", info.State.Msgs)
	}
}

func TestDurableCorruptLedgerStateIsDeadLetteredWithoutResult(t *testing.T) {
	for _, test := range []struct {
		name      string
		corruptKV bool
	}{
		{name: "malformed KV entry", corruptKV: true},
		{name: "malformed stored result"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, natsURL := runEmbeddedNATS(t)
			nc, js := connectJetStream(t, natsURL)
			defer nc.Close()
			addDurableJobStreams(t, js, "CORRUPT_JOBS", "corrupt.jobs", "corrupt.dlq")
			addDurableResultStream(t, js, "CORRUPT_RESULTS", "corrupt.results")
			addDurableResultBucket(t, js, "CORRUPT_RESULTS_KV")
			resultSub, err := js.PullSubscribe("corrupt.results", "corrupt-result-observer", nats.BindStream("CORRUPT_RESULTS"))
			if err != nil {
				t.Fatalf("subscribe to results: %v", err)
			}
			defer resultSub.Unsubscribe()
			dlqSub, err := js.PullSubscribe("corrupt.dlq", "corrupt-dlq-observer", nats.BindStream("CORRUPT_JOBS_DLQ"))
			if err != nil {
				t.Fatalf("subscribe to DLQ: %v", err)
			}
			defer dlqSub.Unsubscribe()

			jobID := "job-corrupt-state"
			storageKey := durableResultStorageKeyForTest(jobID)
			kv, err := js.KeyValue("CORRUPT_RESULTS_KV")
			if err != nil {
				t.Fatalf("open result KV bucket: %v", err)
			}
			if test.corruptKV {
				if _, err := kv.Put("entry."+storageKey, []byte("{")); err != nil {
					t.Fatalf("write malformed KV fixture: %v", err)
				}
			} else {
				coordinator, err := tnats.NewKVIdempotencyCoordinator(js, "CORRUPT_RESULTS_KV")
				if err != nil {
					t.Fatalf("create result coordinator: %v", err)
				}
				claim, err := coordinator.Claim(t.Context(), storageKey, "", time.Minute)
				if err != nil || claim.State != action.IdempotencyClaimAcquired {
					t.Fatalf("claim result key: claim=%+v error=%v", claim, err)
				}
				if err := coordinator.Complete(t.Context(), storageKey, claim.Token, action.IdempotencyEntry{
					Status: 200,
					Body:   []byte("{"),
				}, 0); err != nil {
					t.Fatalf("store malformed result fixture: %v", err)
				}
			}

			var calls atomic.Int64
			binding := tnats.DurableWork("CORRUPT_JOBS", "corrupt.jobs", "corrupt-worker", "corrupt.dlq").
				WithDeliveryPolicy(3, time.Second, 25*time.Millisecond, 8).
				WithDeliveryIdentityField("job_id").WithResult("CORRUPT_RESULTS", "corrupt.results")
			worker := action.New("work.corrupt", func(_ context.Context, _ map[string]any) (any, error) {
				calls.Add(1)
				return map[string]any{"must_not_run": true}, nil
			}).Route(binding).Build()
			tr := tnats.New(natsURL, tnats.WithKVIdempotencyBucket("CORRUPT_RESULTS_KV"))
			tr.Mount([]action.AnyAction{worker})
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
			defer stopTransport(t, tr, cancel)
			go func() { _, _ = tr.Do(ctx, nil) }()
			if err := tr.WaitReady(ctx); err != nil {
				t.Fatalf("wait for worker: %v", err)
			}

			jobData := []byte(`{"job_id":"job-corrupt-state"}`)
			if _, err := js.Publish("corrupt.jobs", jobData); err != nil {
				t.Fatalf("publish corrupt-state job: %v", err)
			}
			var dead tnats.DeadLetter
			if err := json.Unmarshal(fetchOne(t, dlqSub).Data, &dead); err != nil {
				t.Fatalf("decode DLQ record: %v", err)
			}
			if !bytes.Equal(dead.Payload, jobData) || dead.Deliveries != 1 || dead.Error == "" {
				t.Fatalf("DLQ record = %#v, want original payload and first-delivery corruption report", dead)
			}
			if calls.Load() != 0 {
				t.Fatalf("action calls = %d, want no execution for corrupt stored state", calls.Load())
			}
			info, err := js.StreamInfo("CORRUPT_RESULTS")
			if err != nil {
				t.Fatalf("inspect result stream: %v", err)
			}
			if info.State.Msgs != 0 {
				t.Fatalf("result messages = %d, want no fabricated outcome", info.State.Msgs)
			}
		})
	}
}

func TestDurableTransientClaimFailuresCanExhaustDeliveryThreshold(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, js := connectJetStream(t, natsURL)
	defer nc.Close()
	addDurableJobStreams(t, js, "CLAIM_FAILURE_JOBS", "claim-failure.jobs", "claim-failure.dlq")
	addDurableResultStream(t, js, "CLAIM_FAILURE_RESULTS", "claim-failure.results")
	addDurableResultBucket(t, js, "CLAIM_FAILURE_KV")

	resultSub, err := js.PullSubscribe("claim-failure.results", "claim-failure-result-observer", nats.BindStream("CLAIM_FAILURE_RESULTS"))
	if err != nil {
		t.Fatalf("subscribe to results: %v", err)
	}
	defer resultSub.Unsubscribe()
	dlqSub, err := js.PullSubscribe("claim-failure.dlq", "claim-failure-dlq-observer", nats.BindStream("CLAIM_FAILURE_JOBS_DLQ"))
	if err != nil {
		t.Fatalf("subscribe to DLQ: %v", err)
	}
	defer dlqSub.Unsubscribe()
	advisorySub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.CONSUMER.MSG_NAKED.>")
	if err != nil {
		t.Fatalf("subscribe to NAK advisories: %v", err)
	}
	defer advisorySub.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush NAK advisory subscription: %v", err)
	}

	bucketInfo, err := js.StreamInfo("KV_CLAIM_FAILURE_KV")
	if err != nil {
		t.Fatalf("inspect claim bucket: %v", err)
	}
	originalMaxMsgSize := bucketInfo.Config.MaxMsgSize
	bucketInfo.Config.MaxMsgSize = 1
	if _, err := js.UpdateStream(&bucketInfo.Config); err != nil {
		t.Fatalf("force claim write failure: %v", err)
	}

	var calls atomic.Int64
	binding := tnats.DurableWork("CLAIM_FAILURE_JOBS", "claim-failure.jobs", "claim-failure-worker", "claim-failure.dlq").
		WithDeliveryPolicy(2, time.Second, 25*time.Millisecond, 8).
		WithDeliveryIdentityField("job_id").WithResult("CLAIM_FAILURE_RESULTS", "claim-failure.results")
	worker := action.New("work.claim-failure", func(_ context.Context, _ map[string]any) (any, error) {
		calls.Add(1)
		return map[string]any{"unexpected": true}, nil
	}).Route(binding).Build()
	tr := tnats.New(natsURL, tnats.WithKVIdempotencyBucket("CLAIM_FAILURE_KV"))
	tr.Mount([]action.AnyAction{worker})
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer stopTransport(t, tr, cancel)
	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("wait for worker: %v", err)
	}

	jobData := []byte(`{"job_id":"job-claim-failure"}`)
	if _, err := js.Publish("claim-failure.jobs", jobData); err != nil {
		t.Fatalf("publish job: %v", err)
	}
	for range 2 {
		if _, err := advisorySub.NextMsg(3 * time.Second); err != nil {
			t.Fatalf("transient claim failure did not NAK the source: %v", err)
		}
	}

	bucketInfo.Config.MaxMsgSize = originalMaxMsgSize
	if _, err := js.UpdateStream(&bucketInfo.Config); err != nil {
		t.Fatalf("restore claim bucket capacity: %v", err)
	}

	resultMsg := fetchOne(t, resultSub)
	var envelope durableResultTestEnvelope
	if err := json.Unmarshal(resultMsg.Data, &envelope); err != nil {
		t.Fatalf("decode result envelope: %v", err)
	}
	if envelope.JobID != "job-claim-failure" || envelope.Status != "indeterminate" || envelope.Error == nil {
		t.Fatalf("result envelope = %#v, want indeterminate outcome after transient claim failures", envelope)
	}
	var dead tnats.DeadLetter
	if err := json.Unmarshal(fetchOne(t, dlqSub).Data, &dead); err != nil {
		t.Fatalf("decode DLQ record: %v", err)
	}
	if !bytes.Equal(dead.Payload, jobData) || dead.Error == "" {
		t.Fatalf("DLQ record = %#v, want original job and indeterminate error", dead)
	}
	if calls.Load() != 0 {
		t.Fatalf("action calls = %d, want no execution after transient claim failures", calls.Load())
	}
}

func TestDurablePublishesIndeterminateAfterOutcomeStoreRecovery(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, js := connectJetStream(t, natsURL)
	defer nc.Close()
	addDurableJobStreams(t, js, "INDETERMINATE_JOBS", "indeterminate.jobs", "indeterminate.dlq")
	addDurableResultStream(t, js, "INDETERMINATE_RESULTS", "indeterminate.results")
	addDurableResultBucket(t, js, "INDETERMINATE_RESULTS_KV")
	resultSub, err := js.PullSubscribe("indeterminate.results", "indeterminate-result-observer", nats.BindStream("INDETERMINATE_RESULTS"))
	if err != nil {
		t.Fatalf("subscribe to results: %v", err)
	}
	defer resultSub.Unsubscribe()
	dlqSub, err := js.PullSubscribe("indeterminate.dlq", "indeterminate-dlq-observer", nats.BindStream("INDETERMINATE_JOBS_DLQ"))
	if err != nil {
		t.Fatalf("subscribe to DLQ: %v", err)
	}
	defer dlqSub.Unsubscribe()
	advisorySub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.CONSUMER.MSG_NAKED.>")
	if err != nil {
		t.Fatalf("subscribe to NAK advisories: %v", err)
	}
	defer advisorySub.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush NAK advisory subscription: %v", err)
	}

	secondAttempt := make(chan struct{})
	releaseSecondAttempt := make(chan struct{})
	var attempts atomic.Int64
	binding := tnats.DurableWork("INDETERMINATE_JOBS", "indeterminate.jobs", "indeterminate-worker", "indeterminate.dlq").
		WithDeliveryPolicy(2, 5*time.Second, 2*time.Second, 8).
		WithDeliveryIdentityField("job_id").WithResult("INDETERMINATE_RESULTS", "indeterminate.results")
	worker := action.New("work.indeterminate", func(ctx context.Context, _ map[string]any) (any, error) {
		attempt := attempts.Add(1)
		if attempt == 2 {
			close(secondAttempt)
			select {
			case <-releaseSecondAttempt:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return nil, errors.New("temporary action failure")
	}).Route(binding).Build()
	tr := tnats.New(natsURL, tnats.WithKVIdempotencyBucket("INDETERMINATE_RESULTS_KV"))
	tr.Mount([]action.AnyAction{worker})
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer stopTransport(t, tr, cancel)
	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("wait for worker: %v", err)
	}

	jobID := "job-indeterminate"
	jobData := []byte(`{"job_id":"job-indeterminate"}`)
	if _, err := js.Publish("indeterminate.jobs", jobData); err != nil {
		t.Fatalf("publish job: %v", err)
	}
	if _, err := advisorySub.NextMsg(5 * time.Second); err != nil {
		t.Fatalf("first action failure did not NAK: %v", err)
	}
	select {
	case <-secondAttempt:
	case <-ctx.Done():
		t.Fatalf("second action attempt did not start: %v", ctx.Err())
	}

	bucketInfo, err := js.StreamInfo("KV_INDETERMINATE_RESULTS_KV")
	if err != nil {
		t.Fatalf("inspect outcome bucket: %v", err)
	}
	originalMaxMsgSize := bucketInfo.Config.MaxMsgSize
	bucketInfo.Config.MaxMsgSize = 1
	if _, err := js.UpdateStream(&bucketInfo.Config); err != nil {
		t.Fatalf("force completion-store failure: %v", err)
	}
	close(releaseSecondAttempt)
	if _, err := advisorySub.NextMsg(5 * time.Second); err != nil {
		t.Fatalf("failed completion did not NAK the source: %v", err)
	}

	bucketInfo.Config.MaxMsgSize = originalMaxMsgSize
	if _, err := js.UpdateStream(&bucketInfo.Config); err != nil {
		t.Fatalf("restore outcome bucket capacity: %v", err)
	}
	kv, err := js.KeyValue("INDETERMINATE_RESULTS_KV")
	if err != nil {
		t.Fatalf("open outcome bucket: %v", err)
	}
	storageKey := durableResultStorageKeyForTest(jobID)
	claimKey := "claim." + storageKey
	claimEntry, err := kv.Get(claimKey)
	if err != nil {
		t.Fatalf("read retained claim: %v", err)
	}
	var claim struct {
		Token       string `json:"token"`
		RequestHash string `json:"req_hash"`
		ExpiresAt   int64  `json:"exp"`
	}
	if err := json.Unmarshal(claimEntry.Value(), &claim); err != nil {
		t.Fatalf("decode retained claim: %v", err)
	}
	claim.ExpiresAt = 1
	claimData, err := json.Marshal(claim)
	if err != nil {
		t.Fatalf("encode expired claim: %v", err)
	}
	if _, err := kv.Update(claimKey, claimData, claimEntry.Revision()); err != nil {
		t.Fatalf("expire retained claim for redelivery: %v", err)
	}

	resultMsg := fetchOne(t, resultSub)
	var envelope durableResultTestEnvelope
	if err := json.Unmarshal(resultMsg.Data, &envelope); err != nil {
		t.Fatalf("decode indeterminate result: %v", err)
	}
	if envelope.JobID != jobID || envelope.Status != "indeterminate" || envelope.Error == nil || string(envelope.Result) != "null" {
		t.Fatalf("result envelope = %#v, want explicit indeterminate outcome", envelope)
	}
	if err := resultMsg.AckSync(); err != nil {
		t.Fatalf("ack indeterminate result: %v", err)
	}
	var dead tnats.DeadLetter
	if err := json.Unmarshal(fetchOne(t, dlqSub).Data, &dead); err != nil {
		t.Fatalf("decode indeterminate DLQ record: %v", err)
	}
	if !bytes.Equal(dead.Payload, jobData) || attempts.Load() != 2 {
		t.Fatalf("DLQ payload = %q, action attempts = %d; want original source and no third execution", dead.Payload, attempts.Load())
	}
}

func TestDurableRedeliversWhenResultPublishFails(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, js := connectJetStream(t, natsURL)
	defer nc.Close()
	addDurableJobStreams(t, js, "RETRY_JOBS", "retry.jobs", "retry.dlq")
	addDurableResultBucket(t, js, "RETRY_RESULTS_KV")
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:      "RETRY_RESULTS",
		Subjects:  []string{"retry.results"},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Discard:   nats.DiscardNew,
		MaxBytes:  1,
	}); err != nil {
		t.Fatalf("add constrained result stream: %v", err)
	}
	advisorySub, err := nc.SubscribeSync("$JS.EVENT.ADVISORY.CONSUMER.MSG_NAKED.>")
	if err != nil {
		t.Fatalf("subscribe to NAK advisories: %v", err)
	}
	defer advisorySub.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush NAK advisory subscription: %v", err)
	}

	var attempts atomic.Int64
	binding := tnats.DurableWork("RETRY_JOBS", "retry.jobs", "retry-worker", "retry.dlq").
		WithDeliveryPolicy(1, time.Second, 25*time.Millisecond, 8).
		WithDeliveryIdentityField("job_id").WithResult("RETRY_RESULTS", "retry.results")
	worker := action.New("work.retry-result", func(_ context.Context, _ map[string]any) (any, error) {
		attempts.Add(1)
		return map[string]any{"completed": true}, nil
	}).Route(binding).Build()
	tr := tnats.New(natsURL, tnats.WithKVIdempotencyBucket("RETRY_RESULTS_KV"))
	tr.Mount([]action.AnyAction{worker})
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer stopTransport(t, tr, cancel)
	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("wait for worker: %v", err)
	}
	if _, err := js.Publish("retry.jobs", []byte(`{"job_id":"job-retry-result"}`)); err != nil {
		t.Fatalf("publish job: %v", err)
	}
	if _, err := advisorySub.NextMsg(3 * time.Second); err != nil {
		t.Fatalf("result publish failure did not NAK the source job: %v", err)
	}
	if _, err := advisorySub.NextMsg(3 * time.Second); err != nil {
		t.Fatalf("result publish outage did not retry beyond MaxDeliver: %v", err)
	}
	info, err := js.StreamInfo("RETRY_RESULTS")
	if err != nil {
		t.Fatalf("inspect constrained result stream: %v", err)
	}
	info.Config.MaxBytes = 1 << 20
	if _, err := js.UpdateStream(&info.Config); err != nil {
		t.Fatalf("restore result stream capacity: %v", err)
	}
	resultSub, err := js.PullSubscribe("retry.results", "retry-result-observer", nats.BindStream("RETRY_RESULTS"))
	if err != nil {
		t.Fatalf("subscribe to results: %v", err)
	}
	defer resultSub.Unsubscribe()
	result := fetchOne(t, resultSub)
	var envelope durableResultTestEnvelope
	if err := json.Unmarshal(result.Data, &envelope); err != nil {
		t.Fatalf("decode result envelope: %v", err)
	}
	if envelope.JobID != "job-retry-result" || envelope.Status != "succeeded" {
		t.Fatalf("result envelope = %#v, want correlated retry success", envelope)
	}
	var output map[string]any
	if err := json.Unmarshal(envelope.Result, &output); err != nil || output["completed"] != true {
		t.Fatalf("replayed result = %#v, error = %v; want original successful outcome", output, err)
	}
	if got, want := string(result.Data), `{"job_id":"job-retry-result","status":"succeeded","result":{"completed":true},"error":null}`; got != want {
		t.Fatalf("replayed envelope bytes = %s, want exact stored outcome %s", got, want)
	}
	if attempts.Load() != 1 {
		t.Fatalf("action attempts = %d, want exactly one execution across the output outage", attempts.Load())
	}
}

func TestDurableResultStreamMustExistAndCoverSubjectAtStartup(t *testing.T) {
	for _, test := range []struct {
		name          string
		stream        string
		createResults bool
		subjects      []string
		createInput   bool
	}{
		{name: "absent"},
		{name: "wrong subject", stream: "WRONG_RESULTS", createResults: true, subjects: []string{"elsewhere.results"}},
		{name: "missing job stream", stream: "VALID_RESULTS", createResults: true, subjects: []string{"startup.results"}},
		{name: "missing DLQ stream", stream: "VALID_RESULTS", createResults: true, subjects: []string{"startup.results"}, createInput: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, natsURL := runEmbeddedNATS(t)
			nc, js := connectJetStream(t, natsURL)
			defer nc.Close()
			stream := test.stream
			if stream == "" {
				stream = "MISSING_RESULTS"
			}
			if test.createResults {
				addDurableResultStream(t, js, stream, test.subjects...)
			}
			if test.createInput {
				addPersistentTestStream(t, js, "STARTUP_JOBS", "startup.jobs")
			}
			addDurableResultBucket(t, js, "STARTUP_RESULTS_KV")
			binding := tnats.DurableWork("STARTUP_JOBS", "startup.jobs", "startup-worker", "startup.dlq").
				WithDeliveryIdentityField("job_id").WithResult(stream, "startup.results")
			worker := action.New("startup.worker", func(_ context.Context, in any) (any, error) { return in, nil }).Route(binding).Build()
			tr := tnats.New(natsURL, tnats.WithKVIdempotencyBucket("STARTUP_RESULTS_KV"))
			tr.Mount([]action.AnyAction{worker})
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			_, err := tr.Do(ctx, nil)
			if err == nil {
				t.Fatal("transport started with absent or non-covering result stream")
			}
			if err := tr.Close(); err != nil {
				t.Errorf("close transport: %v", err)
			}
		})
	}
}

func TestPublishDurableResultModeValidatesStreamsWithoutProvisioning(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, js := connectJetStream(t, natsURL)
	defer nc.Close()
	addDurableResultStream(t, js, "PUBLISH_RESULTS", "publish.results")
	tr := tnats.New(natsURL)
	defer tr.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	binding := tnats.DurableWork("PUBLISH_JOBS", "publish.jobs", "publish-worker", "publish.dlq").
		WithDeliveryIdentityField("job_id").WithResult("PUBLISH_RESULTS", "publish.results")
	if err := tr.PublishDurable(ctx, binding, map[string]any{"job_id": "publish-1"}); err == nil {
		t.Fatal("PublishDurable succeeded without pre-provisioned job and DLQ streams")
	}
	for _, name := range []string{"PUBLISH_JOBS", "PUBLISH_JOBS_DLQ"} {
		if _, err := js.StreamInfo(name); err == nil {
			t.Fatalf("result-mode publisher created stream %q", name)
		}
	}
	addDurableJobStreams(t, js, "PUBLISH_JOBS", "publish.jobs", "publish.dlq")
	if err := tr.PublishDurable(ctx, binding, map[string]any{"job_id": "publish-1"}); err != nil {
		t.Fatalf("PublishDurable with valid pre-provisioned streams: %v", err)
	}
}

func TestDurableResultsRequireKVBucketAtStartup(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, js := connectJetStream(t, natsURL)
	defer nc.Close()
	addDurableJobStreams(t, js, "NO_KV_JOBS", "no-kv.jobs", "no-kv.dlq")
	addDurableResultStream(t, js, "NO_KV_RESULTS", "no-kv.results")
	binding := tnats.DurableWork("NO_KV_JOBS", "no-kv.jobs", "no-kv-worker", "no-kv.dlq").
		WithDeliveryIdentityField("job_id").WithResult("NO_KV_RESULTS", "no-kv.results")
	worker := action.New("no-kv.worker", func(_ context.Context, in any) (any, error) { return in, nil }).Route(binding).Build()
	tr := tnats.New(natsURL)
	tr.Mount([]action.AnyAction{worker})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := tr.Do(ctx, nil)
	if err == nil {
		t.Fatal("result listener started without a shared KV outcome bucket")
	}
	if err := tr.Close(); err != nil {
		t.Errorf("close transport: %v", err)
	}
}

func TestDurableResultAliasReusesStoredOutcome(t *testing.T) {
	_, natsURL := runEmbeddedNATS(t)
	nc, js := connectJetStream(t, natsURL)
	defer nc.Close()
	addDurableJobStreams(t, js, "ALIAS_JOBS", "alias.jobs", "alias.dlq")
	addDurableResultStream(t, js, "ALIAS_RESULTS", "alias.results")
	addDurableResultBucket(t, js, "ALIAS_RESULTS_KV")
	resultSub, err := js.PullSubscribe("alias.results", "alias-result-observer", nats.BindStream("ALIAS_RESULTS"))
	if err != nil {
		t.Fatalf("subscribe to results: %v", err)
	}
	defer resultSub.Unsubscribe()

	var calls atomic.Int64
	binding := tnats.DurableWork("ALIAS_JOBS", "alias.jobs", "alias-worker", "alias.dlq").
		WithDeliveryIdentityField("job_id").WithResult("ALIAS_RESULTS", "alias.results")
	worker := action.New("work.alias", func(_ context.Context, req map[string]any) (any, error) {
		calls.Add(1)
		return map[string]any{"value": req["value"]}, nil
	}).Route(binding).Build()
	tr := tnats.New(natsURL, tnats.WithKVIdempotencyBucket("ALIAS_RESULTS_KV"))
	tr.Mount([]action.AnyAction{worker})
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer stopTransport(t, tr, cancel)
	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("wait for worker: %v", err)
	}
	for range 2 {
		if _, err := js.Publish("alias.jobs", []byte(`{"job_id":"same-job-id","value":9}`)); err != nil {
			t.Fatalf("publish alias job: %v", err)
		}
	}
	result := fetchOne(t, resultSub)
	if err := result.AckSync(); err != nil {
		t.Fatalf("ack result: %v", err)
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(4 * time.Second)
	defer deadline.Stop()
	for {
		info, err := js.ConsumerInfo("ALIAS_JOBS", "alias-worker")
		if err == nil && info.NumPending == 0 && info.NumAckPending == 0 {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("alias jobs did not settle: info=%#v err=%v", info, err)
		case <-ctx.Done():
			t.Fatalf("context ended before alias jobs settled: %v", ctx.Err())
		case <-ticker.C:
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("action calls = %d, want one execution for duplicate job_id aliases", got)
	}
	info, err := js.StreamInfo("ALIAS_RESULTS")
	if err != nil {
		t.Fatalf("inspect result stream: %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("result messages = %d, want one deduplicated result for aliases", info.State.Msgs)
	}
}

func addDurableResultStream(t *testing.T, js nats.JetStreamContext, name string, subjects ...string) {
	t.Helper()
	addPersistentTestStream(t, js, name, subjects...)
}

func addDurableResultBucket(t *testing.T, js nats.JetStreamContext, name string) {
	t.Helper()
	if _, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  name,
		History: 1,
		Storage: nats.FileStorage,
	}); err != nil {
		t.Fatalf("create durable result KV bucket: %v", err)
	}
}

func addDurableJobStreams(t *testing.T, js nats.JetStreamContext, stream, subject, dlqSubject string) {
	t.Helper()
	addPersistentTestStream(t, js, stream, subject)
	addPersistentTestStream(t, js, stream+"_DLQ", dlqSubject)
}

func addPersistentTestStream(t *testing.T, js nats.JetStreamContext, name string, subjects ...string) {
	t.Helper()
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:      name,
		Subjects:  subjects,
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Discard:   nats.DiscardNew,
	}); err != nil {
		t.Fatalf("add persistent stream: %v", err)
	}
}

func connectJetStream(t *testing.T, url string) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		t.Fatalf("JetStream: %v", err)
	}
	return nc, js
}

func fetchOne(t *testing.T, sub *nats.Subscription) *nats.Msg {
	t.Helper()
	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("fetch message: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("fetched %d messages, want one", len(msgs))
	}
	return msgs[0]
}

func stopTransport(t *testing.T, tr *tnats.Transport, cancel context.CancelFunc) {
	t.Helper()
	cancel()
	if err := tr.Close(); err != nil {
		t.Errorf("close transport: %v", err)
	}
}

func durableResultStorageKeyForTest(jobID string) string {
	hash := sha256.Sum256([]byte(jobID))
	return "durable-result." + hex.EncodeToString(hash[:])
}
