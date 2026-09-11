# Nexss Transport NATS (`tnats`)

Production NATS and JetStream transport adapter for the [Nexss Kernel](https://github.com/nexssp/kernel) action ecosystem.

`transportnats` connects `kernel/action` actions to NATS subjects, request-reply RPC, JetStream durable work queues, pull consumers, Key-Value watchers, ObjectStore events, and NATS Micro Services. It preserves Kernel action execution, error taxonomy, idempotency, request context, tracing identifiers, and graceful lifecycle management across process boundaries.

[![CI](https://github.com/nexssp/transportnats/actions/workflows/ci.yml/badge.svg)](https://github.com/nexssp/transportnats/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/nexssp/transportnats)](https://goreportcard.com/report/github.com/nexssp/transportnats)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

## What this package is

The package is a **transport adapter**, not a replacement for `nexssp/kernel`. Kernel owns action definitions, typed execution, error taxonomy, idempotency contracts, and context metadata. This adapter owns NATS connectivity, subject bindings, JetStream infrastructure, message encoding, ACK/NAK/termination decisions, DLQ envelopes, and broker lifecycle.

The primary package is:

```go
import "github.com/nexssp/transportnats/tnats"
```

The transport accepts `action.AnyAction` values created by `github.com/nexssp/kernel/action` and mounts the action's bindings:

```go
act := action.New("inventory.check", handler).
    Route(tnats.Request("inventory.check.rpc", 2*time.Second)).
    Build()

tr := tnats.New("nats://127.0.0.1:4222")
tr.Mount([]action.AnyAction{act})
```

## Capability matrix

| Capability | Binding or API | Delivery semantics | Typical use |
|---|---|---|---|
| Core Pub/Sub | `tnats.Topic` | At-most-once | Events, notifications, fanout |
| Queue Pub/Sub | `tnats.Topic(subject, queue)` | At-most-once, load-balanced | Competing stateless workers |
| Request/Reply | `tnats.Request` and `Transport.Request` | Request-scoped RPC | Synchronous service calls |
| Durable work | `tnats.DurableWork` and `PublishDurable` | At-least-once, ACK/NAK, DLQ | Payments, orders, workflows |
| Pull consumer | `tnats.Consumer` | Configurable delivery policy | Replay, filters, rate limits |
| JetStream KV | `tnats.KV` / `tnats.WatchKV` | Change notifications | Distributed configuration |
| ObjectStore | `tnats.ObjectStore` | Mutation notifications | Blobs, reports, artifacts |
| NATS Micro | `tnats.Service` | Native discovery and RPC | Broker-native microservices |
| Idempotency | `NewKVIdempotencyCoordinator` | Distributed claim/complete | Duplicate request protection |
| Agent mesh | `examples/05_intensive_agent_mesh` | Mixed RPC, events, queues, and durable audit | Large distributed agent topologies |
| Embedded broker | `WithEmbeddedServer` | In-process NATS/JetStream | Tests and local demos |

## Installation

```bash
go get github.com/nexssp/transportnats
```

Requirements:

- Go `1.26+` as declared by `go.mod`;
- NATS Server `2.10+` for the general feature set;
- NATS Server `2.14+` is recommended for the complete tested feature set, including current multi-filter behavior;
- JetStream enabled for durable work, consumers, KV, ObjectStore, or idempotency.

The package includes the embedded NATS server dependency for `WithEmbeddedServer`. Production deployments normally connect to an externally managed NATS cluster.

## Quickstart: Kernel action with RPC and Pub/Sub

The action definition is the application boundary. A single action can expose multiple routes.

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/nexssp/kernel/action"
    "github.com/nexssp/transportnats/tnats"
)

type CheckRequest struct {
    SKU string `json:"sku"`
    Qty int    `json:"qty"`
}

type CheckResponse struct {
    Available bool `json:"available"`
}

func main() {
    check := action.New("inventory.check", func(ctx context.Context, req CheckRequest) (CheckResponse, error) {
        return CheckResponse{Available: req.Qty <= 100}, nil
    }).
        Route(tnats.Topic("inventory.updated", "inventory-workers")).
        Route(tnats.Request("inventory.check.rpc", 2*time.Second)).
        Build()

    ctx := context.Background()
    tr := tnats.New("nats://127.0.0.1:4222")
    tr.Mount([]action.AnyAction{check})

    go func() {
        if _, err := tr.Do(ctx, nil); err != nil { log.Fatal(err) }
    }()
    if err := tr.WaitReady(ctx); err != nil { log.Fatal(err) }

    var response CheckResponse
    if err := tr.Request(ctx, "inventory.check.rpc", CheckRequest{SKU: "SKU-1", Qty: 3}, &response); err != nil {
        log.Fatal(err)
    }
    log.Printf("available=%v", response.Available)
}
```

`Transport.Request` automatically serializes the request, propagates Kernel context headers, maps remote `xerr` responses back to application errors, and decodes the typed response.

## Reliability semantics

The transport intentionally exposes different delivery guarantees rather than hiding them behind one abstraction.

### Core NATS

Core NATS is at-most-once. A message can be lost if the subscriber is unavailable or the connection fails before delivery. Use it for ephemeral events and notifications where replay is not required.

### JetStream durable work

Durable work is at-least-once. A handler must be safe to execute more than once. Successful executions are ACKed. Failures are retried using the configured backoff or delay. When the maximum delivery count is reached, the transport publishes a structured `DeadLetter` envelope and terminates the original message.

A DLQ is not a substitute for operational recovery. Store enough business identity in the payload to reconcile a dead-lettered operation and monitor the DLQ subject.

### Pull consumers

`ConsumerBinding` supports:

- `DeliverAll`, `DeliverNew`, `DeliverLast`;
- start sequence and start time;
- last-per-subject;
- explicit, none, or all ACK policies;
- multiple filter subjects;
- backoff and rate limits;
- headers-only delivery;
- optional DLQ routing.

Fetches use one context deadline per request. The transport closes subscriptions before joining consumer workers during shutdown.

## JetStream durable work example

```go
binding := tnats.DurableWork(
    "PAYMENTS_STREAM",
    "payments.process",
    "payment-worker-v1",
    "payments.dlq",
).WithDeliveryPolicy(
    5,                  // maximum deliveries
    30*time.Second,     // ACK wait
    time.Second,        // retry delay
    64,                 // max ACK pending
)

processPayment := action.New("payments.process", handlePayment).
    Route(binding).
    Build()

tr := tnats.New("nats://nats.internal:4222")
tr.Mount([]action.AnyAction{processPayment})
```

Publish durable work through the transport:

```go
if err := tr.PublishDurable(ctx, binding, PaymentTask{
    AccountID: "acct-123",
    Amount:    49.90,
}); err != nil {
    return err
}
```

`PublishDurable` verifies or creates the stream and DLQ infrastructure, serializes the payload with the configured codec, and publishes with the caller's context.

## Distributed idempotency with Kernel

Kernel action idempotency can use a JetStream KV coordinator instead of a process-local memory store:

```go
js := tr.JetStream()
coordinator, err := tnats.NewKVIdempotencyCoordinator(js, "NEXSS_IDEMPOTENCY")
if err != nil {
    return err
}

idempotentAction := action.New("orders.create", createOrder).
    Idempotent().
    Route(tnats.Request("orders.create", 3*time.Second)).
    Build()

tr := tnats.New(
    "nats://nats.internal:4222",
    tnats.WithIdempotencyStore(coordinator),
)
tr.Mount([]action.AnyAction{idempotentAction})
```

The coordinator provides atomic claim acquisition, request-hash conflict detection, lease expiry, completion, and token-checked release. Completed entries honor the configured per-action TTL. Idempotency prevents duplicate action execution while a valid claim is held; it does not make arbitrary external side effects exactly-once. Use a transactional business write or outbox for that guarantee.

## Key-Value watcher

```go
configAction := action.New("config.apply", applyConfig).
    Route(tnats.KV("RUNTIME_CONFIG", "runtime.settings")).
    Build()
```

The watcher reconnects with bounded backoff and stops when the transport context is canceled. Create the bucket through deployment migrations or before mounting the action.

## ObjectStore watcher

```go
assetAction := action.New("asset.ingest", func(ctx context.Context, ev tnats.ObjectEvent) (string, error) {
    if ev.Type == tnats.ObjectPut {
        return processBytes(ev.Data), nil
    }
    return "deleted", nil
}).Route(
    tnats.ObjectStore("ASSETS", "reports.>").WithData(10 * 1024 * 1024),
).Build()
```

ObjectStore delivery includes object name, mutation type, size, chunk count, timestamp, and optionally bounded object data. The `*` wildcard matches one token; `>` matches the remainder of the object name.

Direct operations are also available:

```go
info, err := tr.PutObject(ctx, "ASSETS", "reports/2026.csv", data)
data, err := tr.GetObject(ctx, "ASSETS", "reports/2026.csv")
err = tr.DeleteObject(ctx, "ASSETS", "reports/2026.csv")
```

Use `WithData(maxBytes)` deliberately. Large blobs should usually be streamed or processed through a separate storage workflow rather than copied into every action invocation.

## NATS Micro Services

`tnats.Service(service, version, endpoint, subject)` registers a Kernel action as a NATS Micro endpoint and supports native service discovery, ping, info, and stats subjects.

```go
health := action.New("pricing.quote", quote).
    Route(tnats.Service("pricing", "1.0.0", "quote", "pricing.quote")).
    Build()
```

Use the four-service example for a complete order, inventory, payment, and fulfillment choreography.

## Context propagation and errors

The adapter propagates these Kernel values through NATS headers when present:

- `RequestID`;
- `ExecutionID`;
- `TraceID`;
- `SpanID`.

Remote action errors are encoded with the transport error marker and reconstructed through the Kernel `xerr` taxonomy. Do not parse error strings to identify application conditions; use `errors.As` and the Kernel error types.

## Security and production configuration

Use TLS and credentials in production:

```go
tr := tnats.New(
    "tls://nats.internal:4222",
    tnats.WithProductionSecurity(tnats.ProductionSecurity{
        RootCAFile:      "/etc/nats/ca.pem",
        CredentialsFile: "/etc/nats/service.creds",
        ClientCertFile:  "/etc/nats/client.pem",     // optional mTLS
        ClientKeyFile:   "/etc/nats/client-key.pem", // optional mTLS
    }),
)

if err := tr.ValidateProduction(); err != nil {
    return err
}
```

Supported authentication and transport options include:

- user/password;
- token;
- NKey seed files or seed callbacks;
- JWT credentials files;
- root CA files;
- client certificates and keys;
- multiple NATS URLs for cluster connections;
- arbitrary `nats.Option` values through `WithNATSOptions`.

Never commit credentials, NKey seeds, JWTs, client keys, or private CA material. Use secret management and rotate credentials operationally.

## Configuration options

| Option | Purpose |
|---|---|
| `WithCodec` | Replace the default transport codec |
| `WithIdempotencyStore` | Use memory, KV, or another Kernel idempotency implementation |
| `WithNATSOptions` | Add native NATS client options |
| `WithLogger` | Use an application `slog.Logger` |
| `WithFetchBatch` | Tune pull batch size and fetch deadline |
| `WithStreamConfigModifier` | Apply deployment-specific JetStream stream settings |
| `WithProductionSecurity` | Configure TLS and credentials |
| `WithProductionValidation` | Add application-specific production checks |
| `WithEmbeddedServer` | Run an in-process NATS server for tests and demos |

## Examples: what is covered

The repository contains runnable examples under [`examples/`](examples/):

| Example | Demonstrates | Production gap it intentionally leaves to the application |
|---|---|---|
| `01_pubsub_and_request_reply` | Kernel action routes, topic binding, queue group, RPC server | External NATS provisioning and a separate RPC client |
| `02_jetstream_durable_work` | Durable stream, pull worker, ACK/NAK retry policy, DLQ configuration | Durable DB/outbox, DLQ monitoring, deployment migrations |
| `03_four_microservices` | End-to-end choreography, tracing headers, idempotent order entry, durable consumers, competing workers | Real databases and external payment/logistics providers |
| `04_objectstore_and_kv` | Embedded NATS, KV watcher, ObjectStore watcher, bounded blob data, put/get/delete | External storage policy, access control, retention, large-blob strategy |
| `05_intensive_agent_mesh` | Seven transports, 100 jobs, request/reply validation, queue-group workers, event fanout, durable audit, idempotency, and trace propagation | Capacity planning, external persistence, multi-node failover, and real agent runtimes |

The examples cover the principal public APIs, but they do **not** demonstrate every operational concern. In particular, there is no runnable certificate-generation environment, multi-node failover test, external observability backend, database transaction/outbox implementation, or production deployment manifest. Those belong in integration and deployment repositories rather than pretending a local demo proves them.

### Intensive agent mesh

`05_intensive_agent_mesh` is the topology stress example. It starts an embedded JetStream broker and seven independent transports:

1. a gateway using an idempotent request/reply endpoint;
2. a planner consuming a queue-group command topic;
3. a scheduler validating commands through a synchronous request/reply endpoint;
4. two competing worker transports consuming the same task topic;
5. an auditor consuming a durable JetStream stream with retry policy and DLQ;
6. a collector consuming result events through another queue-group topic.

Each submitted job travels through RPC, Core NATS events, queue-group load balancing, and durable JetStream audit delivery. The example submits 100 jobs and waits for all 100 results, making it useful for validating topology wiring and delivery semantics before replacing the in-process broker with a real cluster. It is a topology demonstration, not a benchmark or a claim that one process can represent thousands of production agents.

## Local development and testing

Run the complete validation suite:

```bash
gofmt -w .
go test ./...
go test -race ./...
go vet ./...
go test ./... -run='^$' -bench=. -benchmem
```

The suite uses embedded NATS servers and temporary JetStream stores. It covers:

- Core NATS Pub/Sub and queue groups;
- request/reply success and Kernel error propagation;
- durable delivery, retries, and DLQ;
- filtered pull consumers and poison-pill termination;
- ObjectStore lifecycle and maximum payload handling;
- KV watching;
- Micro Service RPC;
- distributed idempotency, conflicts, leases, TTL, and panic recovery;
- embedded server lifecycle;
- invalid authentication behavior;
- shutdown and race safety.

The benchmarks currently guarantee zero allocations only for the specific token-matching and header fast paths. Network and JetStream paths necessarily allocate and perform I/O.

## Architecture

```text
Kernel action
     │
     ├── Topic / Request ──────────────── Core NATS
     ├── DurableWork / Consumer ───────── JetStream
     ├── KV / Idempotency ─────────────── JetStream KV
     ├── ObjectStore ──────────────────── JetStream ObjectStore
     └── Service ──────────────────────── NATS Micro
                    │
             tnats.Transport
                    │
             NATS / JetStream cluster
```

Bindings are value types. They are mounted from action metadata and keep business handlers independent of NATS client details. The transport owns subscriptions and workers and joins them during shutdown.

## Operational checklist

Before production:

- configure TLS or credentials and run `ValidateProduction`;
- use multiple NATS URLs or a cluster URL;
- enable JetStream replicas appropriate to the availability requirement;
- define stream, consumer, KV, and ObjectStore policies through migrations or infrastructure automation;
- set explicit retention, storage, duplicate windows, and maximum payload policies;
- make durable handlers idempotent;
- monitor consumer pending, redelivery, ACK wait, MaxDeliver, and DLQ rates;
- monitor NATS connection reconnects and transport startup failures;
- propagate a stable business idempotency key where duplicate requests are possible;
- use an outbox or transactionally coupled write for external side effects;
- test failover, reconnect, restart, expired leases, and duplicate delivery against the actual deployment topology.

## License

Nexss Ecosystem — Copyright (c) 2018–2026 Marcin Polak and Contributors.
Distributed under the Apache License 2.0. See [`LICENSE`](LICENSE).
