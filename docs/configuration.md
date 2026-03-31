# Configuration

Configuration is loaded by [`pkg/config`](../pkg/config/config.go): optional **`fleuve.toml`**, then **`FLEUVE_*` environment variables** (which override TOML).

Discovery order for the file: explicit `-config` path → `FLEUVE_CONFIG` → `./fleuve.toml`.

---

## `fleuve.toml`

All keys live under **`[fleuve]`**:

```toml
[fleuve]
database_url = "postgresql://user:pass@localhost:5432/fleuve?sslmode=disable"
nats_url = "nats://localhost:4222"

# Runner / throughput
max_inflight = 4
max_events_per_second = 500.0
max_cache_size = 10000

# Features
enable_jetstream = false
enable_truncation = false
enable_reconciliation = false
enable_external_messaging = false
enable_otel = false

snapshot_interval = 0
truncation_min_retention_days = 7
truncation_batch_size = 1000

create_tables = true
outbox_batch_size = 100
outbox_poll_interval = "100ms"

ui_title = ""
```

Boolean env overrides accept `1`, `true`, `yes` (case-insensitive).

---

## Environment variables (`FLEUVE_*`)

| Variable | Maps to | Purpose |
|----------|---------|---------|
| `FLEUVE_CONFIG` | — | Path to TOML file |
| `FLEUVE_DATABASE_URL` | `database_url` | **Required** for gateway, runner, UI |
| `FLEUVE_NATS_URL` | `nats_url` | NATS server URL |
| `FLEUVE_SNAPSHOT_INTERVAL` | `snapshot_interval` | Snapshot frequency (0 = off) |
| `FLEUVE_ENABLE_TRUNCATION` | `enable_truncation` | Event truncation feature |
| `FLEUVE_TRUNCATION_MIN_RETENTION_DAYS` | `truncation_min_retention_days` | Retention window |
| `FLEUVE_TRUNCATION_BATCH_SIZE` | `truncation_batch_size` | Truncation batch size |
| `FLEUVE_MAX_INFLIGHT` | `max_inflight` | Runner concurrency ceiling |
| `FLEUVE_MAX_EVENTS_PER_SECOND` | `max_events_per_second` | Rate limiting hint |
| `FLEUVE_ENABLE_OTEL` | `enable_otel` | Enables tracer init (see OTEL below) |
| `FLEUVE_MAX_CACHE_SIZE` | `max_cache_size` | In-process ephemeral cache size |
| `FLEUVE_ENABLE_JETSTREAM` | `enable_jetstream` | NATS JetStream publish/consume |
| `FLEUVE_ENABLE_RECONCILIATION` | `enable_reconciliation` | Reconciliation feature flag |
| `FLEUVE_ENABLE_EXTERNAL_MESSAGING` | `enable_external_messaging` | External messaging feature flag |
| `FLEUVE_CREATE_TABLES` | `create_tables` | Auto-create tables (if implemented on path) |
| `FLEUVE_OUTBOX_BATCH_SIZE` | `outbox_batch_size` | Outbox batching |
| `FLEUVE_OUTBOX_POLL_INTERVAL` | `outbox_poll_interval` | Go `time.ParseDuration` string |
| `FLEUVE_UI_TITLE` | `ui_title` | Admin UI title / branding hook |

### OpenTelemetry (tracing)

Used by [`pkg/tracing`](../pkg/tracing/otel.go). Tracing is on if **`enable_otel`** is true in TOML **or** `FLEUVE_OTEL_ENABLED=true`.

| Variable | Default | Purpose |
|----------|---------|---------|
| `FLEUVE_OTEL_ENABLED` | `false` | Force-enable alongside TOML |
| `FLEUVE_OTEL_ENDPOINT` | `localhost:4317` | Collector OTLP address |
| `FLEUVE_OTEL_SERVICE_NAME` | `fleuve` | Service name in traces |
| `FLEUVE_OTEL_SAMPLE_RATE` | `1.0` | Sampling fraction |
| `FLEUVE_OTEL_INSECURE` | `true` | gRPC insecure to collector |
| `FLEUVE_OTEL_PROTOCOL` | `grpc` | `grpc` or `http` |

---

## `fleuve-ui` command-line flags

| Flag | Default | Purpose |
|------|---------|---------|
| `-config` | `""` | Path to `fleuve.toml` |
| `-addr` | `:3000` | Listen address |
| `-frontend` | `""` | Directory overriding embedded UI; else `FLEUVE_FRONTEND_DIST` |
| `-api-only` | `false` | No bundled static UI; JSON under `/`, CORS enabled |

`fleuve-gateway` and `fleuve-runner` use `-config` and their own flags (`-addr` on gateway, `-type` on runner).

---

## Runner identity

| Variable | Purpose |
|----------|---------|
| `FLEUVE_RUNNER_NAME` | Stored on activity rows; defaults to hostname if unset |

---

## Practical notes

1. **One database URL** — Gateway, runner, UI, and ad-hoc examples must agree on `FLEUVE_DATABASE_URL` to see the same workflows.  
2. **JetStream** — Requires NATS reachable at `FLEUVE_NATS_URL` and stream layout compatible with the runner (parity with Python).  
3. **Secrets** — Prefer env vars or a secret manager in production; do not commit real URLs.  
