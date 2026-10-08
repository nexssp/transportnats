# 🌀 Nexss Flow (`.nflow`) + NATS service example

**Start with [`service.nflow`](service.nflow): that is the example.** It contains the request schema, validation, quote pipeline, NATS subject binding, and listener. The Go [`main.go`](main.go) is a thin host for secure connection configuration and process lifecycle; it is not where the business example is defined.

## The Flow program

The full, runnable Flow source is [`service.nflow`](service.nflow):

```nflow
@description "🌀 Nexss Flow catalog quote RPC service"

@schema QuoteRequest struct {
    SKU      string `json:"sku"      validate:"required"`
    Quantity int    `json:"quantity" validate:"required"`
}

@profile quote_endpoint :timeout=3s :concurrency=64

@pipeline catalog_quote :profile=quote_endpoint :nats_request="catalog.quote"
  schema.validate @{ name: "QuoteRequest" } ->
  assert(.quantity >= 1, "quantity must be at least 1") ->
  assert(.quantity <= 100, "quantity must be 100 or less") ->
  {
    sku: .sku,
    quantity: .quantity,
    currency: "USD",
    unit_price_cents: 1299,
    total_price_cents: 1299 * .quantity
  }
@end

# Mount Flow pipelines that declare a NATS binding, then block until shutdown.
nats.listen
```

A caller sends JSON to NATS subject `catalog.quote`:

```json
{"sku":"SKU-42","quantity":3}
```

Successful response:

```json
{"sku":"SKU-42","quantity":3,"currency":"USD","unit_price_cents":1299,"total_price_cents":3897}
```

The pipeline checks the request schema and quantity bounds, then computes a deterministic sample quote. Replace the fixed sample price with your own domain logic. `schema.validate` enforces object shape, required fields, and coarse field types; the explicit `assert` actions enforce the numeric range because numeric `gte`/`lte` validation tags are not interpreted as range rules here.

## Run the `.nflow` program

This sample embeds the reviewed `service.nflow` source in its Go host so the program and host lifecycle ship together:

```bash
export NATS_URL='tls://nats.example.net:4222'
export NATS_CREDS_FILE='/run/secrets/catalog-service.creds'
export NATS_ROOT_CA_FILE='/run/secrets/nats-root-ca.pem'
# Optional mTLS: provide both paths or neither.
export NATS_CLIENT_CERT_FILE='/run/secrets/catalog-service.pem'
export NATS_CLIENT_KEY_FILE='/run/secrets/catalog-service-key.pem'

go run ./examples/09_flow_nats
```

Requirements: Go 1.26+, a reachable NATS endpoint with TLS, a NATS `.creds` file, and a trusted CA PEM file. Before dialing, the host checks the URL scheme, required credential/CA paths, parses the CA PEM, and checks the optional client-certificate pair. NATS verifies the credentials and completes the TLS handshake at connection time. Secret contents are never logged.

`NATS_CLIENT_CERT_FILE` and `NATS_CLIENT_KEY_FILE` are optional as a pair. `NATS_NAME` optionally overrides the NATS client name. Credentials and private keys should come from your orchestrator's secret store, not source control or a checked-in `.env` file. The example uses an externally managed broker; provision NATS users and permissions outside the application, granting the service only the subjects it needs (at minimum, subscribe to `catalog.quote`).

## What the Go host does

[`main.go`](main.go) is deliberately infrastructure glue around the `.nflow` program. It creates one configured `nexssflow` bundle, combines it with Flow's standard bundles, embeds `service.nflow`, runs it with `runner.Execute`, and gives `runner.Host` ownership of bundle cleanup. SIGINT/SIGTERM cancels execution, and the host closes the transport with a bounded shutdown context. Do not construct a second transport for the same Flow bundle: listen, publish, and request actions share one transport and lifecycle.

For another Go service, the central setup is:

```go
transportBundle, err := nexssflow.NewBundle(nexssflow.Config{
    URL:        os.Getenv("NATS_URL"),
    Name:       "nexss-flow-catalog-quote",
    CredsFile:  os.Getenv("NATS_CREDS_FILE"),
    RootCAFile: os.Getenv("NATS_ROOT_CA_FILE"),
    Production: true,
})
if err != nil {
    return err
}

flowConfig, err := runner.BuildConfig(append(native.Bundles(), transportBundle))
if err != nil {
    return err
}

host := runner.NewHost(runner.WithShutdownTimeout(15 * time.Second))
if err := host.Own(transportBundle); err != nil {
    return err
}
return host.Run(ctx, func(ctx context.Context) error {
    _, err := runner.Execute(ctx, flowConfig, source, "service.nflow", nil)
    return err
})
```

`NewBundle` returns configuration errors to application code. The Flow `@require` factory uses the same configuration type and turns invalid compile-time bundle options into a startup failure.

## NATS binding reference

| Flow modifier | Meaning |
|---|---|
| `:nats_request="subject"` | Request/reply endpoint |
| `:nats_topic="subject@queue"` | Core NATS subscription; queue group is optional |
| `:nats_durable="stream:subject:durable:dlq"` | Durable JetStream work endpoint |
| `:nats_consumer="stream:subject:durable"` | JetStream consumer endpoint |
| `:nats_kv="bucket:key"` | JetStream KV watcher |
| `:nats_object="bucket:pattern"` | ObjectStore watcher |
| `:nats_micro="service:version:endpoint:subject"` | NATS Micro endpoint |

`nats.listen` auto-discovers actions in the current Flow resolver that have NATS bindings. For tightly scoped hosts, pass an explicit `endpoints` list to `nats.listen`. A Flow binding selects routing; broker-side permissions remain the security boundary.

## Before production rollout

- Replace the sample quote logic and define a stable, versioned request/response contract.
- Set server-side NATS permissions for exact subjects; do not rely on Flow modifiers for authorization.
- Use a dedicated service identity, rotate credentials, and mount secrets read-only.
- Configure NATS cluster TLS, replicas, stream/consumer retention, and infrastructure through your deployment tooling.
- Add application logs, metrics, traces, and health/readiness integration appropriate to your platform.
- Load-test concurrency and payload sizes against the real cluster; `:concurrency=64` and the sample timeout are demonstration defaults, not universal sizing advice.
- If moving work to JetStream, make handlers idempotent, define retry/DLQ recovery, and provision streams/consumers before rollout.
- Test reconnect, failover, rolling restart, shutdown during in-flight requests, and the NATS permission set in staging.

This example uses Core NATS request/reply (not durable delivery); an unavailable service or expired request can result in a caller timeout. Use the repository's durable-work examples where replay and at-least-once delivery are required.
