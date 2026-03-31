<div align="center">
<img src="docs/images/fleuve-go-logo.png" alt="Fleuve Go logo — flowing river streams" width="140" height="140">
</div>

# Fleuve Go

**Event-sourced workflows in Go**, wire-compatible with [Fleuve (Python)](https://github.com/anomaly/fleuve): same PostgreSQL schema, HTTP command shapes, optional NATS JetStream payloads, and admin UI bundle.

| | |
|--|--|
| **Module** | `github.com/doomervibe/fleuve-go` |
| **Go** | 1.25+ |
| **Stores** | PostgreSQL (events, offsets, activities, delays, …) |
| **Streaming** | NATS JetStream when enabled, else PostgreSQL polling |

---

## What you get

- **Durable workflows** — append-only `stored_events`, versioned per `workflow_id`
- **Command gateway** — REST API to create workflows and submit commands
- **Runner** — consumes the stream (JetStream or PG), runs activities, advances offsets safely
- **Admin UI** — vendored Vite `frontend_dist` embedded in `fleuve-ui` (`pkg/uiembed`), same API the Python console uses
- **Activities** — retries, checkpoints, persistence in `activities`
- **Delays & cron** — `delay_schedules` (see Python parity for full semantics)

**Important:** Do not run **Python and Go runners** concurrently on the same stream scope. Use **cutover**, not mixed consumers. See [docs/behavior-and-python-parity.md](docs/behavior-and-python-parity.md).

---

## Documentation

| Guide | Purpose |
|-------|---------|
| [**Getting started**](docs/getting-started.md) | Postgres, migrations, first run, counter example, gateway + UI |
| [**Architecture**](docs/architecture.md) | Components, data flow, diagrams |
| [**Configuration**](docs/configuration.md) | `fleuve.toml`, `FLEUVE_*`, OpenTelemetry, UI flags |
| [**HTTP API**](docs/http-api.md) | Command gateway + admin JSON API |
| [**Packages**](docs/packages.md) | `pkg/*` layout and responsibilities |
| [**Bundled UI**](docs/ui-embed.md) | Vendored admin frontend (`pkg/uiembed`) |
| [**Operations**](docs/operations.md) | Deploy order, migrations, observability |
| [**Python integration**](docs/INTEGRATION.md) | Sharing a DB with Python Fleuve |
| [**Python parity**](docs/behavior-and-python-parity.md) | Ordering, offsets, recovery |

Full index: [docs/README.md](docs/README.md).

---

## Quick start (minimal)

**1. Database** — apply SQL in `migrations/` in filename order (or use Compose below).

**2. Environment**

```bash
export FLEUVE_DATABASE_URL="postgresql://user:pass@localhost:5432/fleuve?sslmode=disable"
# Optional for JetStream mode:
export FLEUVE_NATS_URL="nats://localhost:4222"
export FLEUVE_ENABLE_JETSTREAM=true
```

**3. Binaries**

```bash
go build -o fleuve-runner   ./cmd/runner
go build -o fleuve-gateway ./cmd/gateway
go build -o fleuve-ui      ./cmd/ui
```

**4. Run** (three terminals, same `FLEUVE_DATABASE_URL`)

```bash
./fleuve-runner          # default -type CounterWorkflow
./fleuve-gateway -addr :8080
./fleuve-ui -addr :3000  # http://localhost:3000
```

**5. Smoke test** — create a counter workflow:

```bash
curl -sS -X POST http://localhost:8080/commands/CounterWorkflow \
  -H "Content-Type: application/json" \
  -d '{"workflow_id":"demo-1","command_type":"increment","payload":{"amount":1}}'
```

Populate data without the gateway: [examples/counter/README.md](examples/counter/README.md) (`go run ./examples/counter` from repo root with `FLEUVE_DATABASE_URL` set).

---

## Docker Compose

```bash
docker compose up -d postgres nats fleuve-runner fleuve-gateway fleuve-ui
```

Services and ports match [docker-compose.yml](docker-compose.yml). Optional profiles: `tracing`, `monitoring`.

---

## Built-in workflow: `CounterWorkflow`

Registered by `fleuve-gateway` and `fleuve-runner` via [`pkg/fleuvecmd`](pkg/fleuvecmd/register.go). Gateway command types: **`increment`** (payload `amount`), **`reset`**.

Add more types in **your** module: implement `model.Workflow`, `repo.NewPGXRepo`, register on the gateway, and run the runner with a matching `-type` once wired in `cmd/runner`.

---

## Vendored admin UI

Static assets live under **`pkg/uiembed/dist/`** and are embedded at compile time. To refresh from a Python Fleuve UI build:

```bash
./scripts/vendor-fleuve-ui.sh /path/to/fleuve/ui/frontend_dist
go build -o fleuve-ui ./cmd/ui
```

Override at runtime: `-frontend /path` or `FLEUVE_FRONTEND_DIST`. JSON-only mode: `fleuve-ui -api-only`.

---

## Repository layout

```
cmd/
  gateway/    # REST command API
  runner/     # Stream consumer + activities
  ui/         # Admin API + static UI
migrations/   # PostgreSQL schema (apply in order)
pkg/          # Libraries (see docs/packages.md)
examples/     # counter/, order/
scripts/      # vendor-fleuve-ui.sh
```

---

## License

MIT
