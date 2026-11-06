package tnats_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/xerr"

	"github.com/nexssp/transportnats/tnats"
)

func TestObjectStoreCRUDLazilyConnectsAndRespectsCanceledDelete(t *testing.T) {
	t.Parallel()
	_, natsURL := runEmbeddedNATS(t)

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("connect test setup: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream test setup: %v", err)
	}
	store, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "LAZY_OBJECTS"})
	if err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	tr := tnats.New(natsURL)
	ctx := t.Context()
	payload := []byte("object-store-lazy")
	if _, err := tr.PutObject(ctx, "LAZY_OBJECTS", "entry", payload); err != nil {
		t.Fatalf("PutObject before Do: %v", err)
	}
	got, err := tr.GetObject(ctx, "LAZY_OBJECTS", "entry")
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("GetObject before Do = %q, %v; want %q", got, err, payload)
	}
	if _, err := tr.GetObjectLimited(ctx, "LAZY_OBJECTS", "entry", 3); xerr.KindFrom(err) != xerr.KindBadRequest {
		t.Fatalf("oversized GetObjectLimited error = %v, want BadRequest", err)
	}

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	if err := tr.DeleteObject(canceledCtx, "LAZY_OBJECTS", "entry"); !errors.Is(err, context.Canceled) {
		t.Fatalf("DeleteObject with canceled context = %v, want context.Canceled", err)
	}
	if _, err := store.GetInfo("entry"); err != nil {
		t.Fatalf("canceled DeleteObject removed the object: %v", err)
	}

	if err := tr.DeleteObject(ctx, "LAZY_OBJECTS", "entry"); err != nil {
		t.Fatalf("DeleteObject before Do: %v", err)
	}
	if _, err := store.GetInfo("entry"); !errors.Is(err, nats.ErrObjectNotFound) {
		t.Fatalf("GetInfo after DeleteObject = %v, want ErrObjectNotFound", err)
	}
}
