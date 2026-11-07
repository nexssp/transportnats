# Nexss Transport NATS

<small>Nexss Flow 🌀 ready</small>

<!-- project-status -->

[![CI](https://github.com/nexssp/transportnats/actions/workflows/ci.yml/badge.svg)](https://github.com/nexssp/transportnats/actions/workflows/ci.yml)
[![Latest tag](https://img.shields.io/github/v/tag/nexssp/transportnats?sort=semver&label=latest%20tag)](https://github.com/nexssp/transportnats/tags)
[![Go Reference](https://pkg.go.dev/badge/github.com/nexssp/transportnats.svg)](https://pkg.go.dev/github.com/nexssp/transportnats)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

<!-- /project-status -->

Nexss Transport NATS connects [Nexss Kernel](https://github.com/nexssp/kernel) actions to NATS Core, JetStream, and NATS Micro. Typed action bindings keep application handlers independent of broker APIs while the adapter manages connections, message encoding, context propagation, workers, and shutdown.

> **Scope:** Core NATS Pub/Sub is at-most-once. JetStream durable work is at-least-once and requires handlers that tolerate retries. Idempotency and a dead-letter subject do not make external side effects exactly-once; use transactional writes or an outbox where that guarantee matters.

## Architecture

```
Go application / Nexss Flow
             │
             ▼
       Nexss Kernel actions
   .Route(tnats.Request(...)) · .Route(tnats.Topic(...))
             │
             ▼
       transportnats/tnats
        ├── Core NATS: Pub/Sub, queue groups, request/reply
        ├── JetStream: durable work, pull consumers, KV, ObjectStore
        └── NATS Micro: service discovery and RPC
             │
             ▼
       NATS / JetStream server
```

## Install

Requires **Go 1.27.3 or newer**. The Flow `nats.listen` action reports readiness after connecting, mounting its endpoints, and flushing subscriptions. NATS Server 2.10+ supports the general feature set; 2.14+ is recommended for the full tested feature set. JetStream must be enabled for durable work, pull consumers, KV, ObjectStore, and JetStream-backed idempotency.

```
go get github.com/nexssp/transportnats@latest
```

Production applications normally connect to a managed NATS cluster. The module also includes an embedded server option for tests and local development.

## API overview

Typed route modifiers bind Kernel actions to Core NATS, JetStream, or NATS Micro. Start the transport with `Do`, wait for readiness with `WaitReady`, then use `Transport.Request` for typed request/reply. The API also provides publish operations, consumer bindings, KV and ObjectStore watchers, security options, and coordinated shutdown.

## Transports and packages

| Package | Purpose |
| --- | --- |
| [`tnats`](tnats/) | Core NATS Pub/Sub and queue groups, request/reply, JetStream durable work and pull consumers, KV and ObjectStore read/write helpers and watchers, NATS Micro services, codecs, security options, and idempotency coordination. |
| [`nexssflow`](nexssflow/) | Nexss Flow bundle with NATS publish/request and durable publishing, JetStream KV/ObjectStore actions, and NATS route modifiers. |

### Delivery semantics

| Binding | Delivery / behavior | Typical use |
| --- | --- | --- |
| `tnats.Topic(subject)` | Core NATS, at-most-once | Ephemeral events and notifications |
| `tnats.Topic(subject, queue)` | Core NATS queue group, at-most-once and load-balanced | Competing stateless workers |
| `tnats.Request(subject, timeout)` | Request-scoped RPC | Synchronous service calls |
| `tnats.DurableWork(...)` | JetStream, at-least-once, ACK/NAK and optional DLQ | Work that must be retried or recovered |
| `tnats.Consumer(...)` | JetStream pull consumer with configurable delivery and ACK policies | Replay, filtering, and controlled consumption |
| `tnats.KV(...)` | JetStream value updates; delete/purge markers are ignored | Configuration changes |
| `tnats.ObjectStore(...)` | JetStream object PUT and DELETE events | Object lifecycle events |
| `tnats.Service(...)` | NATS Micro discovery and RPC | Broker-native service endpoints |

For `Consumer` and `DurableWork` without result envelopes, `MaxDeliver` limits action executions; JetStream redelivery itself remains unlimited so a failed dead-letter publish can be retried without discarding the original message. Durable result mode counts every redelivery, including those caused by transient infrastructure failures, toward its configured threshold. Messages are terminated only after the dead-letter publish succeeds. A consumer using `AckNone` cannot be combined with a dead-letter subject because it cannot safely redeliver on failure.

Durable work can opt into a fresh execution identity per delivery with `DurableWork(...).WithDeliveryIdentityField("job_id")`. The configured field must be a non-empty payload string of at most 256 bytes, with no NUL or surrounding whitespace. It becomes the request ID and `Idempotency-Key`, while every delivery—including a redelivery—gets a new execution ID. If the message already has an `Idempotency-Key`, it must match that field. Without this option, identity behavior is unchanged.

The default idempotency store is process-local memory. For workers sharing a durable consumer, use `WithKVIdempotencyBucket("AGENT_IDEMPOTENCY")` with a pre-provisioned JetStream KV bucket shared by all hosts; do not configure a custom store and a bucket at the same time.

Durable results are opt-in with `WithResult("AGENT_RESULTS", "agent.results")` and require `WithKVIdempotencyBucket("AGENT_IDEMPOTENCY")` with a pre-provisioned KV bucket shared by all workers. The job, DLQ, and output streams must already exist and cover their configured subjects; each must use file storage, limits retention, `DiscardNew`, and no `MaxAge`. Listeners and `PublishDurable` validate these streams in result mode and never create them. The listener atomically claims the stable `job_id` and payload identity in KV, then saves the exact terminal envelope bytes before publishing them. Success envelopes contain `job_id`, `status`, `result`, and `error: null`; terminal failures contain a safe public `error`. A duplicate with the same ID and payload reuses the saved envelope without rerunning the action; a different payload for that ID is a conflict and goes to the DLQ without result replay.

If an identical job is already being processed, the new delivery is NAKed and retried after `RetryDelay`; it does not block a worker waiting on the lease. The active claim is periodically renewed while the action runs. If a worker stops, redeliveries continue to recheck the claim and can acquire it after the lease expires. Transient KV or NATS errors while claiming or saving an outcome, and failures publishing to the result stream, NAK the source for retry. Every redelivery counts toward the configured threshold, even if no action ran. Corrupt ledger data and malformed saved envelopes go to the DLQ without a guessed result. A DLQ publish failure NAKs the source so dead-lettering can be retried.

If transient failures consume the threshold, the listener keeps retrying while the claim fails. Once claiming works, it replays any saved outcome first; if none exists and the delivery threshold has been exceeded, the action is skipped and an `indeterminate` envelope is saved and published before the source is dead-lettered. Result delivery is at-least-once, not transactional exactly-once: JetStream message-ID deduplication is time-bounded, so consumers should also deduplicate by `job_id`; external side effects need their own idempotency or transactional outbox. The no-`MaxAge` result stream can still reach its configured size or message limits; monitor capacity and archive results before limits block new publishes.

Completed result records have no expiry, intentionally preserving replay protection; the shared KV bucket grows with each unique `job_id`. Monitor and capacity-plan the bucket and DLQ. Do not add an automatic TTL or `MaxAge` to the ledger. Prune completed `entry.durable-result.*` keys only through an operator-controlled process after the source stream, DLQ, replay archives, and producer retry/replay windows can no longer redeliver that ID and the agreed replay horizon has passed. Never prune an active claim. Removing a completed key ends its alias/replay protection, so a later copy can execute again and publish another result. Keep original DLQ jobs or an archive for diagnosis and recovery, and resolve storage/capacity failures before replaying them.

The transport propagates Kernel request, execution, trace, and span identifiers through message headers. Remote errors are reconstructed through the Kernel error taxonomy rather than requiring callers to parse strings.

## Nexss Flow 🌀 integration

The Flow bundle compiles `.nflow` pipelines into Kernel actions and mounts those with NATS bindings. Configure `url` to point at a reachable NATS server. `nats.listen` discovers NATS-bound actions in the Flow resolver. Binding modifiers are `:nats_topic`, `:nats_request` (also available as `:nats-rpc`), `:nats_durable`, `:nats_consumer`, `:nats_kv`, `:nats_object`, and `:nats_micro`. `:nats_durable` accepts `stream:subject:durable:dlq`, optionally followed by `:identity-field`, or by both `:identity-field:result-stream:result-subject`. The result form publishes a correlated envelope before source ACK; do not include colons in names or subjects. For `:nats_kv`, use `bucket:key`; an empty key (`bucket:`) watches every key in the bucket. KV binding watchers dispatch value updates and intentionally suppress delete/purge markers; the `nats.kv.delete` action is available for request/response deletes. The Flow action surface includes `nats.listen`, `nats.publish`, `nats.request`, `nats.publish_durable`, `nats.kv.*`, and `nats.object.*`. Bundle options include `url`, `name`, `timeout`, `idempotency_bucket`, credentials/token, TLS files, and `production`; use external secret management for credentials and private keys. Never commit secrets in `.nflow` files.

The following Flow excerpt illustrates a NATS binding; the full runnable service example is in the [centralized Nexss examples repository](https://github.com/nexssp/examples):

```
@require github.com/nexssp/transportnats/nexssflow { url: "nats://127.0.0.1:4222" }

@pipeline catalog_quote :nats_request="catalog.quote"
  {
    sku: .sku,
    quantity: .quantity,
    currency: "USD",
    total_price_cents: 1299 * .quantity
  }
@end

nats.listen
```

### Direct JetStream data actions

These actions connect through the bundle's NATS transport, so a Flow worker can read data written by another process (and vice versa). They operate on existing buckets; create and configure buckets through deployment tooling rather than granting pipeline code bucket-administration rights.

| Action | Request fields | Behavior |
| --- | --- | --- |
| `nats.kv.get` | `bucket`, `key` | Returns `found`, `value`, `revision`, and `created`; missing keys return `found: false`. JSON values are decoded; non-JSON bytes are returned as base64 with `encoding: "base64"`. |
| `nats.kv.put` | `bucket`, `key`, `value` | Unconditional upsert of a JSON-encoded value; returns its new `revision`. |
| `nats.kv.create` | `bucket`, `key`, `value`, optional `ttl` | Creates only if the key is absent. `ttl` is a Go duration string of at least one second (for example, `15m`); per-key TTL is set at creation and cannot be changed later. The bucket/server must support per-key TTL and the bucket must have `LimitMarkerTTL` configured. |
| `nats.kv.update` | `bucket`, `key`, `value`, `revision` | Compare-and-set update; a stale revision returns a conflict instead of overwriting a concurrent change. |
| `nats.kv.delete` | `bucket`, `key`, optional `revision` | Deletes the key; when `revision` is supplied, deletion is conditional on that revision still being current. |
| `nats.object.put` | `bucket`, `name`, `data`, optional `encoding`, `max_bytes` | Stores UTF-8 text by default or base64 bytes with `encoding: "base64"`. Enforces `max_bytes` (8 MiB by default) before upload. |
| `nats.object.get` | `bucket`, `name`, optional `max_bytes` | Reads at most the configured limit plus one byte to detect oversized objects; returns UTF-8 text directly, otherwise base64 with `encoding: "base64"`. |
| `nats.object.delete` | `bucket`, `name` | Deletes the named object. |

Object action `max_bytes` must be zero (use the 8 MiB default) or positive. Raise it explicitly only when the Flow's memory budget and the bucket policy allow larger objects. The existing `:nats_kv` and `:nats_object` modifiers remain event watchers; they are separate from these request/response data actions. The full runnable service example remains in the [centralized Nexss examples repository](https://github.com/nexssp/examples).

## Security and reliability notes

- Use TLS and NATS credentials in production; `WithProductionSecurity` and `ValidateProduction` support startup validation.

- Core NATS does not replay messages missed while a subscriber is unavailable.

- JetStream handlers may run more than once. Keep business operations idempotent and monitor redeliveries and DLQ traffic.

- A JetStream KV or ObjectStore bucket should be provisioned with deployment policy before mounting watchers.

- Idempotency protects action execution while a valid claim is held; it does not make arbitrary external effects transactional.

## Development

Install [Task](https://taskfile.dev), then run the repository's test task or CI gate:

```
task test
task test -- ./tnats
task ci
```

`task test` runs the short test suite; pass a package after `--` to limit it. `task ci` runs lint and uncached race tests, as defined in `Taskfile.yml`. The suite uses embedded NATS/JetStream servers; it does not prove multi-node failover or production deployment behavior.

## Related projects

- [Nexss Kernel](https://github.com/nexssp/kernel) — typed actions and execution contracts.

- [Nexss Flow](https://github.com/nexssp/flow) — flow language and runtime.

- [NATS](https://nats.io/) — messaging system used by this adapter.
