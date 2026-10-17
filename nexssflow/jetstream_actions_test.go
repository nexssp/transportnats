package nexssflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nexssp/kernel/action"

	"github.com/nexssp/transportnats/tnats"
)

func TestFlowJetStreamActionsRegistered(t *testing.T) {
	library := Library(tnats.New("nats://127.0.0.1:4222"), Config{})
	want := []string{
		"nats.kv.get", "nats.kv.put", "nats.kv.create", "nats.kv.update", "nats.kv.delete",
		"nats.object.put", "nats.object.get", "nats.object.delete",
	}
	found := make(map[string]bool, len(library.Actions))
	for _, act := range library.Actions {
		if meta := act.Describe(); meta != nil {
			found[meta.Name] = true
		}
	}
	for _, name := range want {
		if !found[name] {
			t.Errorf("Flow library is missing action %q", name)
		}
	}
}

func TestFlowJetStreamActionInputValidation(t *testing.T) {
	library := Library(tnats.New("nats://127.0.0.1:4222"), Config{})
	cases := []struct {
		name  string
		input any
	}{
		{"nats.kv.get", map[string]any{"bucket": ""}},
		{"nats.kv.update", map[string]any{"bucket": "B", "key": "k", "value": 1}},
		{"nats.kv.create", map[string]any{"bucket": "B", "key": "k", "value": 1, "ttl": "0s"}},
		{"nats.kv.create", map[string]any{"bucket": "B", "key": "k", "value": 1, "ttl": "500ms"}},
		{"nats.object.put", map[string]any{"bucket": "B", "name": "x", "data": "123", "max_bytes": -1}},
		{"nats.object.put", map[string]any{"bucket": "B", "name": "x", "data": "!not-base64!", "encoding": "base64"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			act := actionByName(t, library.Actions, tc.name)
			if _, err := action.InvokeAny(context.Background(), act, tc.input); err == nil {
				t.Fatal("expected invalid input to be rejected")
			}
		})
	}
}

func TestFlowJetStreamActionsCrossConnectionIntegration(t *testing.T) {
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve embedded NATS port: %v", err)
	}
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("reserved NATS address has type %T, want *net.TCPAddr", listener.Addr())
	}
	port := addr.Port
	if err := listener.Close(); err != nil {
		t.Fatalf("release reserved NATS port: %v", err)
	}
	tr := tnats.New("", tnats.WithEmbeddedServer(tnats.EmbeddedConfig{
		Port:      port,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}))
	listenCtx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := tr.Do(listenCtx, nil)
		done <- err
	}()
	readyCtx, cancelReady := context.WithTimeout(context.Background(), 5*time.Second)
	if err := tr.WaitReady(readyCtx); err != nil {
		cancelReady()
		stop()
		waitForNATSFlowTransport(t, done)
		t.Fatalf("start embedded JetStream transport: %v", err)
	}
	cancelReady()
	defer func() {
		stop()
		waitForNATSFlowTransport(t, done)
	}()

	writer, err := nats.Connect(tr.Conn().ConnectedUrl(), nats.Name("separate-flow-test-writer"))
	if err != nil {
		t.Fatalf("connect separate writer: %v", err)
	}
	defer writer.Close()
	writerJS, err := writer.JetStream()
	if err != nil {
		t.Fatalf("create writer JetStream context: %v", err)
	}
	modernWriterJS, err := jetstream.New(writer)
	if err != nil {
		t.Fatalf("create modern writer JetStream context: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := modernWriterJS.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "FLOW_KV",
		LimitMarkerTTL: time.Minute,
	}); err != nil {
		t.Fatalf("create KV bucket: %v", err)
	}
	if _, err := writerJS.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "FLOW_OBJECTS"}); err != nil {
		t.Fatalf("create ObjectStore bucket: %v", err)
	}

	library := Library(tr, Config{Timeout: 3 * time.Second})
	flow := make(map[string]action.AnyAction, len(library.Actions))
	for _, act := range library.Actions {
		if meta := act.Describe(); meta != nil {
			flow[meta.Name] = act
		}
	}
	invoke := func(name string, input any) (any, error) {
		t.Helper()
		return action.InvokeAny(ctx, flow[name], input)
	}

	writerKV, err := modernWriterJS.KeyValue(ctx, "FLOW_KV")
	if err != nil {
		t.Fatalf("open writer KV bucket: %v", err)
	}
	if _, err := writerKV.Put(ctx, "external", []byte(`{"owner":"other-process","count":3}`)); err != nil {
		t.Fatalf("write KV value from separate connection: %v", err)
	}
	got, err := invoke("nats.kv.get", map[string]any{"bucket": "FLOW_KV", "key": "external"})
	if err != nil {
		t.Fatalf("Flow reads KV written by separate connection: %v", err)
	}
	var getRes KVGetResponse
	decodeActionResult(t, got, &getRes)
	value, ok := getRes.Value.(map[string]any)
	if !ok {
		t.Fatalf("KV value type = %T, want object", getRes.Value)
	}
	if !getRes.Found || getRes.Revision == 0 || value["owner"] != "other-process" {
		t.Fatalf("unexpected cross-connection KV result: %+v", getRes)
	}
	missing, err := invoke("nats.kv.get", map[string]any{"bucket": "FLOW_KV", "key": "missing"})
	if err != nil {
		t.Fatalf("Flow get for absent KV key: %v", err)
	}
	decodeActionResult(t, missing, &getRes)
	if getRes.Found {
		t.Fatalf("missing key reported as present: %+v", getRes)
	}

	put, err := invoke("nats.kv.put", map[string]any{
		"bucket": "FLOW_KV", "key": "flow-written", "value": map[string]any{"status": "ready"},
	})
	if err != nil {
		t.Fatalf("Flow KV put: %v", err)
	}
	var writeRes KVWriteResponse
	decodeActionResult(t, put, &writeRes)
	externalEntry, err := writerKV.Get(ctx, "flow-written")
	if err != nil || string(externalEntry.Value()) != `{"status":"ready"}` {
		t.Fatalf("separate connection did not see Flow KV write: entry=%v err=%v", externalEntry, err)
	}

	created, err := invoke("nats.kv.create", map[string]any{
		"bucket": "FLOW_KV", "key": "cas", "value": map[string]any{"v": 1},
	})
	if err != nil {
		t.Fatalf("Flow KV create: %v", err)
	}
	decodeActionResult(t, created, &writeRes)
	firstRevision := writeRes.Revision
	updated, err := invoke("nats.kv.update", map[string]any{
		"bucket": "FLOW_KV", "key": "cas", "value": map[string]any{"v": 2}, "revision": firstRevision,
	})
	if err != nil {
		t.Fatalf("Flow KV compare-and-set update: %v", err)
	}
	decodeActionResult(t, updated, &writeRes)
	if writeRes.Revision <= firstRevision {
		t.Fatalf("updated revision %d did not advance beyond %d", writeRes.Revision, firstRevision)
	}
	if _, err := invoke("nats.kv.update", map[string]any{
		"bucket": "FLOW_KV", "key": "cas", "value": map[string]any{"v": 3}, "revision": firstRevision,
	}); err == nil {
		t.Fatal("stale KV revision unexpectedly passed compare-and-set")
	}
	if _, err := invoke("nats.kv.create", map[string]any{
		"bucket": "FLOW_KV", "key": "cas", "value": "duplicate",
	}); err == nil {
		t.Fatal("KV create unexpectedly replaced an existing key")
	}
	if _, err := invoke("nats.kv.delete", map[string]any{
		"bucket": "FLOW_KV", "key": "cas", "revision": firstRevision,
	}); err == nil {
		t.Fatal("stale KV revision unexpectedly passed conditional delete")
	}
	if _, err := invoke("nats.kv.delete", map[string]any{
		"bucket": "FLOW_KV", "key": "cas", "revision": writeRes.Revision,
	}); err != nil {
		t.Fatalf("conditional KV delete with current revision: %v", err)
	}

	if _, err := invoke("nats.kv.create", map[string]any{
		"bucket": "FLOW_KV", "key": "expiring", "value": "short-lived", "ttl": "1s",
	}); err != nil {
		t.Fatalf("Flow KV create with per-key TTL: %v", err)
	}
	deadline := time.Now().Add(4 * time.Second)
	for {
		_, err = writerKV.Get(ctx, "expiring")
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("per-key TTL did not expire; latest read error: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	objectPut, err := invoke("nats.object.put", map[string]any{
		"bucket": "FLOW_OBJECTS", "name": "text/from-flow", "data": "from a Flow action", "max_bytes": 128,
	})
	if err != nil {
		t.Fatalf("Flow ObjectStore text put: %v", err)
	}
	if _, ok := objectPut.(map[string]any); !ok {
		t.Fatalf("object put response type = %T, want map", objectPut)
	}
	writerObjects, err := writerJS.ObjectStore("FLOW_OBJECTS")
	if err != nil {
		t.Fatalf("open writer ObjectStore: %v", err)
	}
	text, err := writerObjects.GetBytes("text/from-flow")
	if err != nil || string(text) != "from a Flow action" {
		t.Fatalf("separate connection did not see Flow object write: data=%q err=%v", text, err)
	}

	binary := []byte{0, 0xff, 0x01, 0x7f}
	_, err = invoke("nats.object.put", map[string]any{
		"bucket": "FLOW_OBJECTS", "name": "binary/from-flow", "data": "AP8Bfw==", "encoding": "base64",
	})
	if err != nil {
		t.Fatalf("Flow ObjectStore binary put: %v", err)
	}
	gotBinary, err := writerObjects.GetBytes("binary/from-flow")
	if err != nil || !bytes.Equal(gotBinary, binary) {
		t.Fatalf("binary object mismatch from separate connection: %v, err=%v", gotBinary, err)
	}
	got, err = invoke("nats.object.get", map[string]any{"bucket": "FLOW_OBJECTS", "name": "binary/from-flow"})
	if err != nil {
		t.Fatalf("Flow ObjectStore binary get: %v", err)
	}
	var binaryRes ObjectGetResponse
	decodeActionResult(t, got, &binaryRes)
	if binaryRes.Data != "AP8Bfw==" || binaryRes.Encoding != "base64" || binaryRes.Size != int64(len(binary)) {
		t.Fatalf("unexpected binary ObjectStore response: %+v", binaryRes)
	}
	if _, err := invoke("nats.object.put", map[string]any{
		"bucket": "FLOW_OBJECTS", "name": "too-large", "data": "12345", "max_bytes": 4,
	}); err == nil {
		t.Fatal("oversized ObjectStore upload was not rejected")
	}
	if _, err := invoke("nats.object.get", map[string]any{
		"bucket": "FLOW_OBJECTS", "name": "binary/from-flow", "max_bytes": 3,
	}); err == nil {
		t.Fatal("oversized ObjectStore read was not rejected")
	}

	fromWriter := []byte("stored outside Flow")
	if _, err := writerObjects.PutBytes("text/from-writer", fromWriter); err != nil {
		t.Fatalf("write ObjectStore object from separate connection: %v", err)
	}
	got, err = invoke("nats.object.get", map[string]any{"bucket": "FLOW_OBJECTS", "name": "text/from-writer"})
	if err != nil {
		t.Fatalf("Flow reads ObjectStore object from separate connection: %v", err)
	}
	var objectRes ObjectGetResponse
	decodeActionResult(t, got, &objectRes)
	if objectRes.Data != string(fromWriter) || objectRes.Encoding != "utf-8" || objectRes.Size != int64(len(fromWriter)) {
		t.Fatalf("unexpected ObjectStore read response: %+v", objectRes)
	}

	if _, err := invoke("nats.object.delete", map[string]any{"bucket": "FLOW_OBJECTS", "name": "text/from-writer"}); err != nil {
		t.Fatalf("Flow ObjectStore delete: %v", err)
	}
	if _, err := writerObjects.GetBytes("text/from-writer"); err == nil {
		t.Fatal("ObjectStore object still exists after Flow delete")
	}
}

func actionByName(t *testing.T, actions []action.AnyAction, name string) action.AnyAction {
	t.Helper()
	for _, act := range actions {
		if meta := act.Describe(); meta != nil && meta.Name == name {
			return act
		}
	}
	t.Fatalf("action %q not found", name)
	return nil
}

func decodeActionResult(t *testing.T, value, target any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal action result: %v", err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		t.Fatalf("decode action result: %v", err)
	}
}

func waitForNATSFlowTransport(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("NATS Flow transport exited with error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for NATS Flow transport shutdown")
	}
}
