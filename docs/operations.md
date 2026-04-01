# Internal operations (Fleuve Go)

Checklist for running and maintaining this repo in a trusted environment. This is not a guide for exposing services to the public internet.

**Documentation index:** [docs/README.md](./README.md) · [Getting started](./getting-started.md) · [Configuration](./configuration.md) · [HTTP API](./http-api.md)

**Runner semantics** (ordering, offsets, recovery) and **parity with Python** are described in [behavior-and-python-parity.md](./behavior-and-python-parity.md). Only **one** active runner stack (Python *or* Go) should process a given stream scope—not both at once.

## Deploy order

1. **PostgreSQL** — apply migrations under `migrations/` before starting processors that write events.
2. **NATS JetStream** — stream and KV layout must match what the runner expects (see main Fleuve docs / Python parity).
3. **Runner** (`fleuve-runner`) — long-lived consumer: `-type CounterWorkflow` (default) matches the built-in aggregate in `pkg/counterworkflow`. Uses NATS when `[fleuve] enable_jetstream = true` and `nats_url` is set; otherwise polls `stored_events` with a reader key `{WorkflowName}_pg` and persists NATS offsets under `{WorkflowName}_nats` when JetStream is enabled.
4. **Gateway** (`fleuve-gateway`) — wires CounterWorkflow + `ActionExecutor` (activity rows in `activities`, structured logs via `log/slog`). Extend `pkg/fleuvecmd` or your own `main` to add types.
5. **UI** (`fleuve-ui` / `go run ./examples/ui_server`) — optional; reference server composing **`pkg/uibackend`** + **`pkg/uiembed`** (vendored `frontend_dist`). Refresh assets with `./scripts/vendor-fleuve-ui.sh` then rebuild.

## Configuration

- Primary: `fleuve.toml` plus `FLEUVE_*` environment overrides (same keys as the Python implementation).
- In CI and Docker, unset or align `FLEUVE_*` when tests assert TOML-only behavior (see `pkg/config` tests).

## Migrations

Run SQL in `migrations/` in filename order against the target database. Do not skip versions when upgrading an existing cluster.

## Integration tests

- Compose file: `docker-compose.test.yml` (and workflow in `.github/workflows/go.yml`).
- Tests that need real Postgres/NATS are tagged or live under `integration_test/`; run with the documented compose stack when applicable.

## Observability

- **OpenTelemetry**: tracing turns on if **`enable_otel`** is true in `[fleuve]` **or** `FLEUVE_OTEL_ENABLED=true`. Endpoint, protocol, and sampling use `FLEUVE_OTEL_*` (see `pkg/tracing`). Collector image is pinned in `docker-compose.yml`.
- **Prometheus**: scrape config in `prometheus.yml`; Prometheus service uses a pinned image tag in compose.

## Activities and recovery

- The executor writes `activities` rows (running / retrying / completed / failed) when `WithActivityPersistence` is used (gateway and runner). A periodic job re-queues **pending / running / retrying** rows by reloading events from `stored_events`.
- Failed-action HTTP retry requires `RegisterWorkflowModel` for that workflow type (done for CounterWorkflow in `pkg/fleuvecmd`).

## Quick health checks

- Gateway: HTTP readiness or a known command endpoint as documented in the main README.
- DB: connectivity and migration version.
- NATS: JetStream available and streams created.
