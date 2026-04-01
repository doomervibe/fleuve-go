# Getting started

This walkthrough gets **PostgreSQL**, **optional NATS**, and the three shipped binaries (`fleuve-runner`, `fleuve-gateway`, `fleuve-ui`) running locally. It uses the built-in **CounterWorkflow**.

---

## Prerequisites

| Requirement | Notes |
|-------------|--------|
| **Go 1.25+** | See `go.mod` |
| **PostgreSQL** | 13+ recommended; same schema as Python Fleuve |
| **NATS** (optional) | 2.10+ with JetStream if `enable_jetstream=true` |

---

## 1. Create the database

Create an empty database (example: `fleuve`). Then apply migrations **in lexicographic order** under [`migrations/`](../migrations/):

```bash
psql "$FLEUVE_DATABASE_URL" -f migrations/001_initial_schema.up.sql
psql "$FLEUVE_DATABASE_URL" -f migrations/002_add_compression.up.sql
# … any newer *.up.sql files
```

Using Docker:

```bash
docker compose up -d postgres
# Migrations are mounted into the Postgres image in docker-compose.yml for fresh volumes
```

---

## 2. Point the apps at Postgres

All processes that should see the same workflows **must share one** `FLEUVE_DATABASE_URL`.

```bash
export FLEUVE_DATABASE_URL="postgresql://fleuve:secret@localhost:5432/fleuve?sslmode=disable"
```

Optional: create [`fleuve.toml`](../docs/configuration.md#fleuve-toml) in the working directory; environment variables **override** TOML (see [`pkg/config`](../pkg/config/config.go)).

---

## 3. Optional: NATS JetStream

When JetStream is enabled, the gateway can publish committed events after each DB transaction, and the runner consumes from NATS instead of polling `stored_events`.

```bash
export FLEUVE_NATS_URL="nats://localhost:4222"
export FLEUVE_ENABLE_JETSTREAM=true
```

Start NATS with JetStream (see [`docker-compose.yml`](../docker-compose.yml)).

---

## 4. Build binaries

From the repository root:

```bash
go build -o fleuve-runner   ./cmd/runner
go build -o fleuve-gateway ./cmd/gateway
go build -o fleuve-ui      ./examples/ui_server
```

---

## 5. Run the stack

Use **three terminals** (same exports in each).

**Terminal A — runner**

```bash
./fleuve-runner
# default: -type CounterWorkflow
```

**Terminal B — gateway**

```bash
./fleuve-gateway -addr :8080
```

**Terminal C — admin UI**

```bash
./fleuve-ui -addr :3000
```

Open [http://localhost:3000](http://localhost:3000). The UI reads **only** from PostgreSQL; it does not need NATS.

---

## 6. Create a workflow via HTTP

Counter gateway commands are parsed by [`counterworkflow.ParseGatewayCommand`](../pkg/counterworkflow/parse.go): types **`increment`** and **`reset`**.

**Create** (first command for a new id):

```bash
curl -sS -X POST http://localhost:8080/commands/CounterWorkflow \
  -H "Content-Type: application/json" \
  -d '{
    "workflow_id": "demo-1",
    "command_type": "increment",
    "payload": { "amount": 3 }
  }'
```

**Another command** on the same workflow:

```bash
curl -sS -X POST http://localhost:8080/commands/CounterWorkflow/demo-1 \
  -H "Content-Type: application/json" \
  -d '{ "command_type": "increment", "payload": { "amount": 1 } }'
```

**Admin API** (same DB as above):

```bash
curl -sS "http://localhost:3000/api/workflows?workflow_type=CounterWorkflow&limit=10"
```

Empty `[]` usually means the UI is pointed at a **different** database than the gateway/example.

---

## 7. Populate data without the gateway

See [examples/counter/README.md](../examples/counter/README.md): `go run ./examples/counter` with `FLEUVE_DATABASE_URL` set.

---

## Next steps

- [Architecture](./architecture.md) — how writes, NATS, and the runner fit together  
- [HTTP API](./http-api.md) — full route list  
- [Configuration](./configuration.md) — tuning and OpenTelemetry  
- [Python integration](./INTEGRATION.md) — existing Python Fleuve databases  
