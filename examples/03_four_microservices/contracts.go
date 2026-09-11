package main

import "time"

// --- ORDER DOMAIN CONTRACTS ---

type CreateOrderReq struct {
	OrderID   string  `json:"order_id"`
	AccountID string  `json:"account_id"`
	SKU       string  `json:"sku"`
	Quantity  int     `json:"quantity"`
	AmountUSD float64 `json:"amount_usd"`
}

type CreateOrderRes struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}

type OrderCreatedEvent struct {
	OrderID   string    `json:"order_id"`
	AccountID string    `json:"account_id"`
	SKU       string    `json:"sku"`
	Quantity  int       `json:"quantity"`
	AmountUSD float64   `json:"amount_usd"`
	CreatedAt time.Time `json:"created_at"`
}

// --- INVENTORY DOMAIN CONTRACTS ---

type InventoryReservedEvent struct {
	OrderID    string    `json:"order_id"`
	AccountID  string    `json:"account_id"`
	SKU        string    `json:"sku"`
	Quantity   int       `json:"quantity"`
	AmountUSD  float64   `json:"amount_usd"`
	ReservedAt time.Time `json:"reserved_at"`
}

// --- PAYMENT DOMAIN CONTRACTS ---

type PaymentSettledEvent struct {
	OrderID   string    `json:"order_id"`
	AccountID string    `json:"account_id"`
	AmountUSD float64   `json:"amount_usd"`
	TxID      string    `json:"tx_id"`
	SettledAt time.Time `json:"settled_at"`
}

// --- FULFILLMENT DOMAIN CONTRACTS ---

type DispatchConfirmedEvent struct {
	OrderID      string    `json:"order_id"`
	TrackingCode string    `json:"tracking_code"`
	DispatchedAt time.Time `json:"dispatched_at"`
}
