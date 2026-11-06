package tnats_test

import (
	"context"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"

	"github.com/nexssp/transportnats/tnats"
)

func TestRequestBindingTimeoutReachesHandlerContext(t *testing.T) {
	t.Parallel()
	_, natsURL := runEmbeddedNATS(t)

	deadlineSeen := make(chan bool, 1)
	rpcAct := action.New("request.timeout", func(ctx context.Context, req OrderReq) (OrderRes, error) {
		deadline, ok := ctx.Deadline()
		deadlineSeen <- ok && time.Until(deadline) > 0
		return OrderRes{Confirmation: req.ID}, nil
	}).Route(tnats.Request("request.timeout", time.Minute)).Build()

	tr := tnats.New(natsURL)
	tr.Mount([]action.AnyAction{rpcAct})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	go func() { _, _ = tr.Do(ctx, nil) }()
	if err := tr.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	var res OrderRes
	if err := tr.Request(ctx, "request.timeout", OrderReq{ID: "deadline"}, &res); err != nil {
		t.Fatalf("Request: %v", err)
	}
	if res.Confirmation != "deadline" {
		t.Fatalf("response = %q, want deadline", res.Confirmation)
	}
	if !<-deadlineSeen {
		t.Fatal("request handler did not receive an active deadline")
	}
}
