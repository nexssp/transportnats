package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/transportnats/tnats"
)

// BuildOrderService creates the public Gateway Service exposing synchronous RPC
// and emitting events through the transport with Kernel context propagation.
func BuildOrderService(orderTr *tnats.Transport, logger *slog.Logger) action.AnyAction {
	return action.New("order.create", func(ctx context.Context, req CreateOrderReq) (CreateOrderRes, error) {
		if req.Quantity <= 0 || req.AmountUSD <= 0 {
			return CreateOrderRes{}, xerr.BadRequest("invalid quantity or amount")
		}

		// Simulate validation and internal DB write latency
		time.Sleep(20 * time.Millisecond)

		logger.Info("[OrderService] Order received and validated",
			"order_id", req.OrderID,
			"account", req.AccountID,
			"trace_id", action.TraceIDFrom(ctx),
		)

		event := OrderCreatedEvent{
			OrderID:   req.OrderID,
			AccountID: req.AccountID,
			SKU:       req.SKU,
			Quantity:  req.Quantity,
			AmountUSD: req.AmountUSD,
			CreatedAt: time.Now().UTC(),
		}

		// Publish through the transport so Kernel request and trace headers are propagated.
		if err := orderTr.Publish(ctx, "orders.event.created", event); err != nil {
			return CreateOrderRes{}, err
		}

		return CreateOrderRes{OrderID: req.OrderID, Status: "ACCEPTED"}, nil
	}).
		Idempotent().
		Route(tnats.Request("orders.create", 3*time.Second)).
		Build()
}

// BuildInventoryService listens on orders.event.created via a Durable Pull Consumer,
// verifies warehouse stock, and emits inventory.event.reserved.
func BuildInventoryService(inventoryTr *tnats.Transport, logger *slog.Logger) action.AnyAction {
	return action.New("inventory.reserve", func(ctx context.Context, ev OrderCreatedEvent) (string, error) {
		// Simulate warehouse stock lookup & row-level lock latency
		time.Sleep(35 * time.Millisecond)

		logger.Info("[InventoryService] Stock reserved successfully",
			"order_id", ev.OrderID,
			"sku", ev.SKU,
			"qty", ev.Quantity,
			"trace_id", action.TraceIDFrom(ctx),
		)

		reserved := InventoryReservedEvent{
			OrderID:    ev.OrderID,
			AccountID:  ev.AccountID,
			SKU:        ev.SKU,
			Quantity:   ev.Quantity,
			AmountUSD:  ev.AmountUSD,
			ReservedAt: time.Now().UTC(),
		}

		err := inventoryTr.Publish(ctx, "inventory.event.reserved", reserved)

		return "RESERVED", err
	}).
		Route(tnats.Consumer("COMMERCE_STREAM", "orders.event.created", "inventory-consumer").
			NewOnly().
			WithAckPolicy(tnats.AckExplicit)).
		Build()
}

// BuildPaymentService processes payments idempotently and emits payment.event.settled.
func BuildPaymentService(paymentTr *tnats.Transport, logger *slog.Logger) action.AnyAction {
	return action.New("payment.charge", func(ctx context.Context, ev InventoryReservedEvent) (string, error) {
		// Simulate third-party payment gateway latency (e.g. Stripe / Adyen / SEPA)
		time.Sleep(50 * time.Millisecond)

		txID := fmt.Sprintf("txn_%s_%d", ev.OrderID, time.Now().UnixMilli())
		logger.Info("[PaymentService] Payment settled via payment gateway",
			"order_id", ev.OrderID,
			"amount_usd", ev.AmountUSD,
			"tx_id", txID,
			"trace_id", action.TraceIDFrom(ctx),
		)

		settled := PaymentSettledEvent{
			OrderID:   ev.OrderID,
			AccountID: ev.AccountID,
			AmountUSD: ev.AmountUSD,
			TxID:      txID,
			SettledAt: time.Now().UTC(),
		}

		err := paymentTr.Publish(ctx, "payment.event.settled", settled)

		return txID, err
	}).
		Route(tnats.Consumer("COMMERCE_STREAM", "inventory.event.reserved", "payment-consumer").
			NewOnly().
			WithBackOff(50*time.Millisecond, 200*time.Millisecond)).
		Build()
}

// BuildFulfillmentService dispatches physical items using a distributed QueueGroup
// for competing consumer load balancing.
func BuildFulfillmentService(fulfillmentTr *tnats.Transport, onComplete func(), logger *slog.Logger) action.AnyAction {
	return action.New("fulfillment.dispatch", func(ctx context.Context, ev PaymentSettledEvent) (string, error) {
		// Simulate packaging, label generation, and logistics ERP dispatch
		time.Sleep(30 * time.Millisecond)

		trackingCode := fmt.Sprintf("TRACK-DHL-%s", ev.OrderID)
		logger.Info("[FulfillmentService] Package dispatched to carrier",
			"order_id", ev.OrderID,
			"tx_id", ev.TxID,
			"tracking_code", trackingCode,
			"trace_id", action.TraceIDFrom(ctx),
		)

		onComplete()

		return trackingCode, nil
	}).
		Route(tnats.Topic("payment.event.settled", "fulfillment-workers")).
		Build()
}
