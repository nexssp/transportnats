package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/transportnats/tnats"
)

type InventoryReq struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

type InventoryRes struct {
	Available bool `json:"available"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	inventoryAction := action.New("inventory.check", func(ctx context.Context, req InventoryReq) (InventoryRes, error) {
		fmt.Printf("📦 Checking inventory for SKU: %s (Qty: %d)\n", req.SKU, req.Qty)

		return InventoryRes{Available: req.Qty <= 100}, nil
	}).
		Route(tnats.Topic("inventory.updated", "inventory-service")).
		Route(tnats.Request("inventory.rpc", 2*time.Second)).
		Build()

	transport := tnats.New("nats://127.0.0.1:4222")
	transport.Mount([]action.AnyAction{inventoryAction})

	go func() {
		if _, err := transport.Do(ctx, nil); err != nil {
			log.Printf("Transport error: %v", err)
		}
	}()

	fmt.Println("🚀 NATS Transport listening. Press Ctrl+C to stop.")
	<-ctx.Done()
}
