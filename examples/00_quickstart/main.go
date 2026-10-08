package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/transportnats/tnats"
)

type InventoryRequest struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type InventoryResponse struct {
	SKU       string `json:"sku"`
	Available bool   `json:"available"`
	Remaining int    `json:"remaining"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "quickstart:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const stockOnHand = 12

	checkInventory := action.New("inventory.check", func(_ context.Context, req InventoryRequest) (InventoryResponse, error) {
		remaining := stockOnHand - req.Quantity
		return InventoryResponse{
			SKU:       req.SKU,
			Available: req.Quantity > 0 && remaining >= 0,
			Remaining: max(remaining, 0),
		}, nil
	}).
		Description("Check available inventory for a SKU").
		Route(tnats.Request("inventory.check", 2*time.Second)).
		Build()

	transport := tnats.New("", tnats.WithEmbeddedServer(tnats.EmbeddedConfig{
		Port:   -1,
		NoLog:  true,
		NoSigs: true,
	}))
	transport.Mount([]action.AnyAction{checkInventory})

	transportDone := make(chan error, 1)
	go func() {
		_, err := transport.Do(ctx, nil)
		transportDone <- err
	}()

	readyCtx, cancelReady := context.WithTimeout(ctx, 5*time.Second)
	readyErr := transport.WaitReady(readyCtx)
	cancelReady()
	if readyErr != nil {
		cancel()
		<-transportDone
		return fmt.Errorf("wait for NATS transport: %w", readyErr)
	}

	requestCtx, cancelRequest := context.WithTimeout(ctx, 2*time.Second)
	var response InventoryResponse
	requestErr := transport.Request(requestCtx, "inventory.check", InventoryRequest{
		SKU:      "SKU-42",
		Quantity: 3,
	}, &response)
	cancelRequest()

	cancel()
	select {
	case err := <-transportDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("stop NATS transport: %w", err)
		}
	case <-time.After(5 * time.Second):
		return errors.New("timed out waiting for NATS transport shutdown")
	}
	if requestErr != nil {
		return fmt.Errorf("request inventory: %w", requestErr)
	}

	fmt.Printf("SKU %s: available=%t, remaining=%d\n", response.SKU, response.Available, response.Remaining)
	return nil
}
