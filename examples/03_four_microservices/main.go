package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/transportnats/tnats"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 1. Boot up in-process JetStream broker
	storeDir, err := os.MkdirTemp("", "nats_mesh_*")
	if err != nil {
		logger.Error("Failed creating temp store dir", "error", err)

		return
	}
	defer os.RemoveAll(storeDir)

	broker := tnats.New("", tnats.WithEmbeddedServer(tnats.EmbeddedConfig{
		Port:       4222,
		JetStream:  true,
		StoreDir:   storeDir,
		ServerName: "embedded-commerce-broker",
		NoLog:      true,
		NoSigs:     true,
	}))

	go func() {
		if _, err := broker.Do(ctx, nil); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("Broker error", "error", err)
		}
	}()

	if err := broker.WaitReady(ctx); err != nil {
		logger.Error("Broker startup timed out", "error", err)

		return
	}

	natsURL := broker.Conn().ConnectedUrl()
	logger.Info("Embedded NATS JetStream broker ready", "url", natsURL)

	// Ensure JetStream streams exist before consumers bind
	initCommerceStreams(broker.JetStream(), logger)

	var (
		wg                 sync.WaitGroup
		completedPipelines atomic.Int64
	)

	// 2. Initialize 4 independent microservice transports
	orderTr := tnats.New(natsURL)
	inventoryTr := tnats.New(natsURL)
	paymentTr := tnats.New(natsURL)
	fulfillmentTr := tnats.New(natsURL)

	// Mount isolated actions
	orderTr.Mount([]action.AnyAction{BuildOrderService(orderTr, logger)})
	inventoryTr.Mount([]action.AnyAction{BuildInventoryService(inventoryTr, logger)})
	paymentTr.Mount([]action.AnyAction{BuildPaymentService(paymentTr, logger)})
	fulfillmentTr.Mount([]action.AnyAction{BuildFulfillmentService(fulfillmentTr, func() {
		completedPipelines.Add(1)
	}, logger)})

	// Start all microservices concurrently
	startService(ctx, &wg, orderTr, "OrderService", logger)
	startService(ctx, &wg, inventoryTr, "InventoryService", logger)
	startService(ctx, &wg, paymentTr, "PaymentService", logger)
	startService(ctx, &wg, fulfillmentTr, "FulfillmentService", logger)

	_ = orderTr.WaitReady(ctx)
	_ = inventoryTr.WaitReady(ctx)
	_ = paymentTr.WaitReady(ctx)
	_ = fulfillmentTr.WaitReady(ctx)

	logger.Info(">>> 4 Microservices connected and ready. Starting order generation...")

	// 3. Client Simulation: Submit 5 orders via RPC
	clientTr := tnats.New(natsURL)

	clientCtx, clientCancel := context.WithCancel(context.Background())
	go func() { _, _ = clientTr.Do(clientCtx, nil) }()

	_ = clientTr.WaitReady(ctx)

	const totalOrders = 5
	for i := 1; i <= totalOrders; i++ {
		orderID := fmt.Sprintf("ORD-2026-%04d", i)
		req := CreateOrderReq{
			OrderID:   orderID,
			AccountID: "ACC-ENTERPRISE-1",
			SKU:       "DELL-XPS-15",
			Quantity:  1,
			AmountUSD: 2499.00,
		}

		// Inject trace headers for distributed tracing verification
		traceCtx := action.WithTraceContext(ctx, fmt.Sprintf("trace-%s", orderID), "span-root")
		traceCtx = xctx.WithRequestID(traceCtx, fmt.Sprintf("req-%s", orderID))

		var res CreateOrderRes

		err := clientTr.Request(traceCtx, "orders.create", req, &res)
		if err != nil {
			logger.Error("Order RPC failed", "order_id", orderID, "error", err)

			continue
		}

		logger.Info("[Client] Order accepted by gateway", "order_id", res.OrderID, "status", res.Status)

		time.Sleep(15 * time.Millisecond) // realistic client arrival interval
	}

	// 4. Await end-to-end choreography completion
	deadline := time.Now().Add(10 * time.Second)
	for completedPipelines.Load() < int64(totalOrders) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	logger.Info(">>> All pipelines completed",
		"processed", completedPipelines.Load(),
		"expected", totalOrders,
	)

	// 5. Clean, zero-leak shutdown
	clientCancel()
	stop()
	wg.Wait()
	logger.Info("All microservices and broker shut down cleanly. Zero leaks.")
}

func startService(ctx context.Context, wg *sync.WaitGroup, tr *tnats.Transport, name string, log *slog.Logger) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		if _, err := tr.Do(ctx, nil); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("Service error", "service", name, "error", err)
		}
	}()
}

func initCommerceStreams(js nats.JetStreamContext, log *slog.Logger) {
	streamCfg := &nats.StreamConfig{
		Name:      "COMMERCE_STREAM",
		Subjects:  []string{"orders.event.*", "inventory.event.*"},
		Storage:   nats.MemoryStorage,
		Retention: nats.LimitsPolicy,
		MaxAge:    time.Hour,
	}
	if _, err := js.AddStream(streamCfg); err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		log.Error("Failed to initialize JetStream stream", "error", err)
	}
}
