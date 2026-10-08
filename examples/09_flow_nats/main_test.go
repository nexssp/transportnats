package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/transportnats/nexssflow"
	"github.com/nexssp/transportnats/tnats"

	"github.com/nexssp/flow/core"
	"github.com/nexssp/flow/native"
	"github.com/nexssp/flow/runner"
)

func TestFlowNATSServiceRequestAndShutdown(t *testing.T) {
	transport := tnats.New("", tnats.WithEmbeddedServer(tnats.EmbeddedConfig{
		Port:   -1,
		NoLog:  true,
		NoSigs: true,
	}))
	bundle := core.Bundle{
		ID:        nexssflow.ID,
		Libraries: []action.Library{nexssflow.Library(transport, nexssflow.Config{Timeout: time.Second})},
		Modifiers: nexssflow.Modifiers(),
		Shutdowns: []core.ShutdownFunc{func(context.Context) error { return transport.Close() }},
	}
	flowConfig, err := runner.BuildConfig(append(native.Bundles(), bundle))
	if err != nil {
		t.Fatalf("build Flow runtime: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	host := runner.NewHost(runner.WithShutdownTimeout(5 * time.Second))
	if err := host.Own(bundle); err != nil {
		t.Fatalf("own Flow bundle: %v", err)
	}
	source := embeddedFlowSource(t)
	done := make(chan error, 1)
	go func() {
		done <- host.Run(ctx, func(runCtx context.Context) error {
			_, runErr := runner.Execute(runCtx, flowConfig, source, "service.nflow", nil)
			return runErr
		})
	}()

	readyCtx, cancelReady := context.WithTimeout(context.Background(), 5*time.Second)
	if err := transport.WaitReady(readyCtx); err != nil {
		cancelReady()
		cancel()
		waitForService(t, done)
		t.Fatalf("wait for NATS listener readiness: %v", err)
	}
	cancelReady()

	client := tnats.New(transport.Conn().ConnectedUrl())
	defer func() { _ = client.Close() }()
	requestCtx, cancelRequest := context.WithTimeout(context.Background(), 2*time.Second)
	var response struct {
		SKU             string `json:"sku"`
		Quantity        int    `json:"quantity"`
		Currency        string `json:"currency"`
		UnitPriceCents  int    `json:"unit_price_cents"`
		TotalPriceCents int    `json:"total_price_cents"`
	}
	err = client.Request(requestCtx, "catalog.quote", map[string]any{"sku": "SKU-42", "quantity": 3}, &response)
	cancelRequest()
	if err != nil {
		cancel()
		waitForService(t, done)
		t.Fatalf("request Flow quote: %v", err)
	}
	if response.SKU != "SKU-42" || response.Quantity != 3 || response.Currency != "USD" || response.UnitPriceCents != 1299 || response.TotalPriceCents != 3897 {
		t.Fatalf("quote response = %#v", response)
	}

	for _, quantity := range []int{0, 101} {
		invalidCtx, cancelInvalid := context.WithTimeout(context.Background(), 2*time.Second)
		invalidErr := client.Request(invalidCtx, "catalog.quote", map[string]any{"sku": "SKU-42", "quantity": quantity}, &response)
		cancelInvalid()
		if invalidErr == nil {
			cancel()
			waitForService(t, done)
			t.Fatalf("expected Flow assertions to reject quantity %d", quantity)
		}
	}

	cancel()
	if err := waitForService(t, done); err != nil {
		t.Fatalf("Flow host returned error during shutdown: %v", err)
	}
	if transport.Connected() {
		t.Fatal("transport remains connected after Flow host shutdown")
	}
}

func embeddedFlowSource(t *testing.T) string {
	t.Helper()
	source, err := flowSource.ReadFile("service.nflow")
	if err != nil {
		t.Fatalf("read Flow source: %v", err)
	}
	return string(source)
}

func waitForService(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Flow service shutdown")
		return nil
	}
}
