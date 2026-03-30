# Fleuve Go

A faithful Go port of the [Fleuve](https://github.com/anomaly/fleuve) event-sourced workflow framework.

## Overview

Fleuve is a **type-safe, event-sourced workflow framework** with:
- Durable workflows backed by **PostgreSQL** (events as source of truth)
- **NATS** JetStream/KV for ephemeral state caching and messaging
- Horizontal scaling via **partitioning**
- **Activities** (side effects) with retries and checkpoints
- **Delays** and **cron**-based scheduling
- **Snapshots** and **event truncation**
- **Command gateway** (REST)
- **Admin UI** (wire-compatible with Python version)

## Installation

```bash
go get github.com/doomervibe/fleuve-go
```

## Quick Start

### 1. Define Your Workflow

```go
package main

import (
    "github.com/doomervibe/fleuve-go/pkg/model"
)

// Define your state
type MyState struct {
    model.StateBase
    Counter int `json:"counter"`
}

// Define your events
type CounterIncremented struct {
    model.EventBase
    Amount int `json:"amount"`
}

func (e *CounterIncremented) GetType() string { return "counter_incremented" }

// Define your commands
type IncrementCounter struct {
    Amount int `json:"amount"`
}

// Implement the Workflow interface
type MyWorkflow struct{}

func (w *MyWorkflow) Name() string { return "MyWorkflow" }
func (w *MyWorkflow) SchemaVersion() int { return 1 }

func (w *MyWorkflow) Upcast(eventType string, schemaVersion int, rawData map[string]any) map[string]any {
    return rawData
}

func (w *MyWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
    switch c := cmd.(type) {
    case *IncrementCounter:
        return []model.Event{&CounterIncremented{Amount: c.Amount}}, nil
    }
    return nil, nil
}

func (w *MyWorkflow) Evolve(state model.State, event model.Event) model.State {
    var s *MyState
    if state != nil {
        s = state.(*MyState).Copy().(*MyState)
    } else {
        s = &MyState{Counter: 0}
    }
    
    switch e := event.(type) {
    case *CounterIncremented:
        s.Counter += e.Amount
    }
    return s
}

func (w *MyWorkflow) EventToCmd(e model.Event) model.Command {
    return nil
}

func (w *MyWorkflow) IsFinalEvent(e model.Event) bool {
    return false
}
```

### 2. Run the Workflow

```go
package main

import (
    "context"
    "database/sql"
    "log"
    
    "github.com/doomervibe/fleuve-go/pkg/config"
    "github.com/doomervibe/fleuve-go/pkg/repo"
    "github.com/doomervibe/fleuve-go/pkg/runner"
    "github.com/doomervibe/fleuve-go/pkg/stream"
)

func main() {
    cfg, _ := config.LoadFleuveToml("")
    
    db, _ := sql.Open("postgres", cfg.DatabaseURL)
    defer db.Close()
    
    workflow := &MyWorkflow{}
    storage := repo.NewInProcessEphemeralStorage(10000)
    
    repository := repo.NewRepo(
        db,
        workflow.Name(),
        workflow,
        storage,
    )
    
    reader := stream.NewPGReader(db, workflow.Name()+"_runner", 100)
    reader.Init(context.Background())
    
    r := runner.NewWorkflowsRunner(
        workflow,
        repository,
        reader,
        nil, // side effects
        runner.WithMaxInflight(cfg.MaxInflight),
    )
    
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    if err := r.Start(ctx); err != nil {
        log.Fatal(err)
    }
    
    select {}
}
```

### 3. Start the Command Gateway

```bash
go build -o fleuve-gateway ./cmd/gateway
./fleuve-gateway -addr :8080
```

### 4. Start the Admin UI

```bash
# Build the frontend (or use Python's build)
# Then serve:
go build -o fleuve-ui ./cmd/ui
./fleuve-ui -addr :3000 -frontend /path/to/frontend_dist
```

## Configuration

Configuration is loaded from `fleuve.toml` and environment variables:

```toml
[fleuve]
database_url = "postgresql://user:pass@localhost:5432/fleuve"
nats_url = "nats://localhost:4222"
snapshot_interval = 100
enable_truncation = true
max_inflight = 4
max_events_per_second = 500.0
```

Environment variables (prefixed with `FLEUVE_`) override TOML settings:

```bash
export FLEUVE_DATABASE_URL="postgresql://..."
export FLEUVE_MAX_INFLIGHT=8
```

## HTTP APIs

### Command Gateway

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/commands/{workflow_type}` | POST | Create a new workflow |
| `/commands/{workflow_type}/{workflow_id}` | POST | Process a command |
| `/commands/{workflow_type}/{workflow_id}/pause` | POST | Pause a workflow |
| `/commands/{workflow_type}/{workflow_id}/resume` | POST | Resume a workflow |
| `/commands/{workflow_type}/{workflow_id}/cancel` | POST | Cancel a workflow |

### Admin UI API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/api/workflow-types` | GET | List workflow types |
| `/api/workflows` | GET | List workflows |
| `/api/workflows/{id}` | GET | Get workflow details |
| `/api/workflows/{id}/events` | GET | Get workflow events |
| `/api/activities` | GET | List activities |
| `/api/delays` | GET | List delays |
| `/api/stats` | GET | Dashboard statistics |

## Package Structure

```
pkg/
├── actions/      # Action executor with retry/checkpoint logic
├── config/       # TOML + env var configuration
├── delay/        # Cron delay scheduler
├── external/     # External NATS messaging
├── gateway/      # HTTP command gateway
├── metrics/      # Prometheus metrics
├── model/        # Core domain models
├── postgres/     # PostgreSQL ORM models
├── repo/         # Repository (persistence layer)
├── runner/       # Main event loop runner
├── scaling/      # Partition scaling logic
├── stream/       # Event stream readers
├── testing/      # Testing harness
├── tracing/      # OpenTelemetry tracing
├── truncation/   # Event truncation service
└── uibackend/    # Admin UI HTTP API
```

## Python compatibility

This Go port is **wire-compatible** with the [Python Fleuve](https://github.com/anomaly/fleuve) implementation:

- **Same HTTP API endpoints and JSON response shapes**
- **Same PostgreSQL schema** (can run against existing databases)
- **Same NATS JetStream message format**
- **Same configuration keys** (`fleuve.toml` + `FLEUVE_*` env vars)

**Behavior** (ordering, at-least-once vs exactly-once expectations, consumer offsets, recovery): **Python is the reference.** Go should match it; differences are treated as gaps to fix. This repo **does not** support running Python and Go **runners** concurrently on the same stream—use **cutover**, not mixed processing. See [docs/behavior-and-python-parity.md](docs/behavior-and-python-parity.md).

The frontend UI from `fleuve/ui/frontend_dist` works without modification.

## Binaries

| Binary | Description |
|--------|-------------|
| `fleuve-runner` | Consumes the event stream (NATS JetStream when `enable_jetstream=true` and `nats_url` is set; otherwise polls `stored_events` via PostgreSQL), runs activities, and applies `EventToCmd` fan-out. Ships with **CounterWorkflow** (`-type CounterWorkflow`, default). |
| `fleuve-gateway` | REST command API; registers built-in workflows (CounterWorkflow) against PostgreSQL. |
| `fleuve-ui` | Admin UI: reads workflows, events, activities, delays, and stats from **PostgreSQL**; optional static frontend via `-frontend` / `FLEUVE_FRONTEND_DIST`. |

Additional workflow types belong in your module: implement `model.Workflow`, register on the gateway, and pass the same type to the runner’s `-type` flag once wired in `cmd/runner`.

## Dependencies

- `github.com/robfig/cron/v3` - Cron expression parsing
- `github.com/pelletier/go-toml/v2` - TOML configuration

## Operations

For an internal runbook (deploy order, migrations, integration tests, observability), see [docs/operations.md](docs/operations.md). For Python vs Go semantics (ordering, offsets, recovery), see [docs/behavior-and-python-parity.md](docs/behavior-and-python-parity.md).

## License

MIT
