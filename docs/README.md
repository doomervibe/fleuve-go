# Fleuve Go - Complete Documentation

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Installation](#installation)
4. [Quick Start](#quick-start)
5. [Core Concepts](#core-concepts)
6. [Defining Workflows](#defining-workflows)
7. [HTTP APIs](#http-apis)
8. [Configuration](#configuration)
9. [Advanced Topics](#advanced-topics)
10. [Production Deployment](#production-deployment)
11. [API Reference](#api-reference)
12. [Examples](#examples)

---

## Overview

Fleuve is a **type-safe, event-sourced workflow framework** for building durable, scalable workflows. This is the Go port of the Python implementation, maintaining **wire compatibility** (schema, HTTP, NATS payloads, config keys). **Runtime behavior** (ordering, offsets, recovery) is defined by **Python** as the reference; see [behavior-and-python-parity.md](./behavior-and-python-parity.md).

### Key Features

- **Event Sourcing**: All state changes stored as immutable events
- **PostgreSQL Persistence**: Durable storage with ACID guarantees
- **NATS Integration**: Real-time event streaming with JetStream
- **Horizontal Scaling**: Partition-based scaling for high throughput
- **Activity Execution**: Side effects with retries, checkpoints, DLQ
- **Delay & Cron Scheduling**: Time-based workflow triggers
- **Snapshots & Truncation**: Efficient state reconstruction
- **Wire compatible**: Same PostgreSQL schema and UI bundle as Python; **mixed Python+Go runners are not supported**—use cutover (see [behavior-and-python-parity.md](./behavior-and-python-parity.md))

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Clients                               │
│              (HTTP / gRPC / CLI)                             │
└────────────────────────┬────────────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         │               │               │
    ┌────▼────┐    ┌────▼────┐    ┌────▼────┐
    │ Gateway │    │   UI    │    │  CLI    │
    │ :8080   │    │  :3000  │    │         │
    └────┬────┘    └────┬────┘    └─────────┘
         │               │
         └───────┬───────┘
                 │
         ┌───────▼───────┐
         │     Repo      │
         │  (Write Path) │
         └───────┬───────┘
                 │
    ┌────────────┼────────────┐
    │            │            │
┌───▼───┐   ┌───▼───┐   ┌───▼────┐
│Postgres│   │ NATS  │   │Ephemeral│
│ (Events)│  │JetStrm│   │  Cache  │
└────────┘   └───────┘   └─────────┘
    │            │
    └─────┬──────┘
          │
    ┌─────▼──────┐
    │   Runner   │
    │  (Read)    │
    └─────┬──────┘
          │
    ┌─────▼──────┐
    │  Activity  │
    │  Executor  │
    └────────────┘
```

### Data Flow

```
Command → Gateway → Repo → PostgreSQL (Events)
                        ↓
                    NATS JetStream (Publish)
                        ↓
                    Runner (Subscribe)
                        ↓
                    Activity Executor
                        ↓
                    External Systems
```

---

## Installation

```bash
# Clone or add to go.mod
go get github.com/doomervibe/fleuve-go

# Build binaries
cd fleuve-go
go build -o fleuve-runner ./cmd/runner
go build -o fleuve-gateway ./cmd/gateway
go build -o fleuve-ui ./cmd/ui
```

### Dependencies

- Go 1.21+
- PostgreSQL 13+
- NATS Server 2.9+ (with JetStream)

---

## Quick Start

### 1. Define Your Workflow

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    
    "github.com/doomervibe/fleuve-go/pkg/model"
)

// Define your state
type CounterState struct {
    model.StateBase
    Count int `json:"count"`
}

func (s *CounterState) Copy() model.State {
    return &CounterState{
        Count: s.Count,
        StateBase: *s.StateBase.Copy(),
    }
}

// Define events
type CounterIncremented struct {
    model.EventBase
    Amount int `json:"amount"`
}

func (e *CounterIncremented) GetType() string { return "counter_incremented" }

type CounterReset struct {
    model.EventBase
}

func (e *CounterReset) GetType() string { return "counter_reset" }

// Define commands
type IncrementCmd struct {
    Amount int `json:"amount"`
}

type ResetCmd struct{}

// Implement Workflow interface
type CounterWorkflow struct{}

func (w *CounterWorkflow) Name() string { return "CounterWorkflow" }
func (w *CounterWorkflow) SchemaVersion() int { return 1 }

func (w *CounterWorkflow) Upcast(eventType string, schemaVersion int, rawData map[string]any) map[string]any {
    return rawData // No upcasting needed for v1
}

func (w *CounterWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
    // Initialize state if nil
    var s *CounterState
    if state != nil {
        s = state.(*CounterState)
    } else {
        s = &CounterState{Count: 0}
    }
    
    // Check lifecycle
    if s.GetLifecycle() != model.LifecycleActive {
        return nil, &model.Rejection{Msg: "workflow not active"}
    }
    
    // Handle commands
    switch c := cmd.(type) {
    case *IncrementCmd:
        if c.Amount <= 0 {
            return nil, &model.Rejection{Msg: "amount must be positive"}
        }
        return []model.Event{&CounterIncremented{Amount: c.Amount}}, nil
        
    case *ResetCmd:
        return []model.Event{&CounterReset{}}, nil
    }
    
    return nil, nil
}

func (w *CounterWorkflow) Evolve(state model.State, event model.Event) model.State {
    var s *CounterState
    if state != nil {
        s = state.(*CounterState).Copy().(*CounterState)
    } else {
        s = &CounterState{Count: 0}
    }
    
    switch e := event.(type) {
    case *CounterIncremented:
        s.Count += e.Amount
    case *CounterReset:
        s.Count = 0
    }
    
    return s
}

func (w *CounterWorkflow) EventToCmd(e model.Event) model.Command {
    // Map events back to commands for subscriptions
    return nil
}

func (w *CounterWorkflow) IsFinalEvent(e model.Event) bool {
    return false // This workflow never ends
}
```

### 2. Set Up the Repository

```go
package main

import (
    "context"
    "log"
    
    "github.com/doomervibe/fleuve-go/pkg/config"
    "github.com/doomervibe/fleuve-go/pkg/repo"
)

func main() {
    ctx := context.Background()
    
    // Load configuration
    cfg, err := config.LoadFleuveToml("")
    if err != nil {
        log.Fatal(err)
    }
    
    // Create PostgreSQL connection pool
    pool, err := repo.NewPGXPool(ctx, cfg.DatabaseURL, 10)
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Close()
    
    // Create ephemeral storage (LRU cache)
    storage := repo.NewInProcessEphemeralStorage(cfg.MaxCacheSize)
    
    // Create repository
    workflow := &CounterWorkflow{}
    repository := repo.NewPGXRepo(
        pool,
        "CounterWorkflow",
        workflow,
        storage,
    )
    
    // Create workflow
    state, err := repository.CreateNew(ctx, &IncrementCmd{Amount: 5}, "counter-001", nil)
    if err != nil {
        log.Fatal(err)
    }
    
    log.Printf("Created workflow: %s at version %d", state.ID, state.Version)
}
```

### 3. Run the Gateway

```go
package main

import (
    "context"
    "log"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    
    "github.com/doomervibe/fleuve-go/pkg/config"
    "github.com/doomervibe/fleuve-go/pkg/gateway"
    "github.com/doomervibe/fleuve-go/pkg/repo"
)

func main() {
    ctx := context.Background()
    
    cfg, _ := config.LoadFleuveToml("")
    
    pool, _ := repo.NewPGXPool(ctx, cfg.DatabaseURL, 10)
    defer pool.Close()
    
    storage := repo.NewInProcessEphemeralStorage(cfg.MaxCacheSize)
    
    workflow := &CounterWorkflow{}
    repository := repo.NewPGXRepo(pool, "CounterWorkflow", workflow, storage)
    
    // Create gateway
    gw := gateway.NewFleuveCommandGateway()
    
    // Register command parser
    parser := func(cmdType string, payload map[string]any) (model.Command, error) {
        switch cmdType {
        case "increment":
            var cmd IncrementCmd
            b, _ := json.Marshal(payload)
            json.Unmarshal(b, &cmd)
            return &cmd, nil
        case "reset":
            return &ResetCmd{}, nil
        }
        return nil, fmt.Errorf("unknown command type: %s", cmdType)
    }
    
    gw.RegisterWorkflowType("CounterWorkflow", repository, parser)
    
    // Start HTTP server
    mux := http.NewServeMux()
    gw.RegisterRoutes(mux)
    
    server := &http.Server{Addr: ":8080", Handler: mux}
    
    go func() {
        log.Printf("Gateway listening on :8080")
        server.ListenAndServe()
    }()
    
    // Graceful shutdown
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    <-quit
    
    server.Shutdown(context.Background())
}
```

---

## Core Concepts

### Event Sourcing

Every state change is recorded as an immutable event:

```go
// Events are facts that happened
type OrderCreated struct {
    model.EventBase
    OrderID   string    `json:"order_id"`
    Customer  string    `json:"customer"`
    Items     []string  `json:"items"`
    CreatedAt time.Time `json:"created_at"`
}

// State is derived by replaying events
func (w *OrderWorkflow) Evolve(state model.State, event model.Event) model.State {
    var s *OrderState
    if state != nil {
        s = state.(*OrderState)
    } else {
        s = &OrderState{Status: "new"}
    }
    
    switch e := event.(type) {
    case *OrderCreated:
        s.OrderID = e.OrderID
        s.Status = "created"
    case *OrderPaid:
        s.Status = "paid"
    case *OrderShipped:
        s.Status = "shipped"
    }
    
    return s
}
```

### Command-Event Separation

```
┌──────────┐     ┌──────────┐     ┌──────────┐
│ Command  │────▶│  Decide  │────▶│  Events  │
│(Intent)  │     │(Business │     │  (Facts) │
└──────────┘     │  Logic)  │     └──────────┘
                 └──────────┘
                      │
                      ▼
                 ┌──────────┐
                 │  Evolve  │
                 │  State   │
                 └──────────┘
```

**Commands** express intent, **Events** record facts.

### Workflow Lifecycle

```
    ┌─────────┐
    │ Active  │◀─────────────┐
    └────┬────┘              │
         │                   │
    Pause│Resume        Cancel│
         │                   │
         ▼                   │
    ┌─────────┐              │
    │ Paused  │──────────────┘
    └─────────┘
         │
         │ Cancel
         ▼
    ┌──────────┐
    │ Cancelled│
    └──────────┘
```

---

## Defining Workflows

### Workflow Interface

```go
type Workflow interface {
    // Name returns the workflow type identifier
    Name() string
    
    // SchemaVersion returns current schema version for upcasting
    SchemaVersion() int
    
    // Upcast transforms old event formats to current
    Upcast(eventType string, schemaVersion int, rawData map[string]any) map[string]any
    
    // Decide evaluates a command and returns events or rejection
    Decide(state State, cmd Command) ([]Event, *Rejection)
    
    // Evolve applies an event to state, returning new state
    Evolve(state State, event Event) State
    
    // EventToCmd converts events to commands for subscriptions
    EventToCmd(e Event) Command
    
    // IsFinalEvent returns true if this event completes the workflow
    IsFinalEvent(e Event) bool
}
```

### Complete Example: Order Workflow

```go
package main

import (
    "context"
    "encoding/json"
    "time"
    
    "github.com/doomervibe/fleuve-go/pkg/model"
)

// ============ State ============

type OrderState struct {
    model.StateBase
    OrderID     string    `json:"order_id"`
    CustomerID  string    `json:"customer_id"`
    Items       []Item    `json:"items"`
    Total       float64   `json:"total"`
    Status      string    `json:"status"`
    ShippedAt   *time.Time `json:"shipped_at,omitempty"`
    DeliveredAt *time.Time `json:"delivered_at,omitempty"`
}

type Item struct {
    ProductID string  `json:"product_id"`
    Quantity  int     `json:"quantity"`
    Price     float64 `json:"price"`
}

func (s *OrderState) Copy() model.State {
    items := make([]Item, len(s.Items))
    copy(items, s.Items)
    
    return &OrderState{
        StateBase:   *s.StateBase.Copy(),
        OrderID:     s.OrderID,
        CustomerID:  s.CustomerID,
        Items:       items,
        Total:       s.Total,
        Status:      s.Status,
        ShippedAt:   s.ShippedAt,
        DeliveredAt: s.DeliveredAt,
    }
}

// ============ Events ============

type OrderCreated struct {
    model.EventBase
    OrderID    string    `json:"order_id"`
    CustomerID string    `json:"customer_id"`
    Items      []Item    `json:"items"`
    Total      float64   `json:"total"`
    CreatedAt  time.Time `json:"created_at"`
}

func (e *OrderCreated) GetType() string { return "order_created" }

type OrderPaid struct {
    model.EventBase
    PaymentID  string    `json:"payment_id"`
    Amount     float64   `json:"amount"`
    PaidAt     time.Time `json:"paid_at"`
}

func (e *OrderPaid) GetType() string { return "order_paid" }

type OrderShipped struct {
    model.EventBase
    TrackingNumber string    `json:"tracking_number"`
    Carrier        string    `json:"carrier"`
    ShippedAt      time.Time `json:"shipped_at"`
}

func (e *OrderShipped) GetType() string { return "order_shipped" }

type OrderDelivered struct {
    model.EventBase
    DeliveredAt time.Time `json:"delivered_at"`
}

func (e *OrderDelivered) GetType() string { return "order_delivered" }

type OrderCancelled struct {
    model.EventBase
    Reason     string    `json:"reason"`
    CancelledAt time.Time `json:"cancelled_at"`
}

func (e *OrderCancelled) GetType() string { return "order_cancelled" }

// ============ Commands ============

type CreateOrderCmd struct {
    OrderID    string  `json:"order_id"`
    CustomerID string  `json:"customer_id"`
    Items      []Item  `json:"items"`
}

type PayOrderCmd struct {
    PaymentID string  `json:"payment_id"`
    Amount    float64 `json:"amount"`
}

type ShipOrderCmd struct {
    TrackingNumber string `json:"tracking_number"`
    Carrier        string `json:"carrier"`
}

type DeliverOrderCmd struct{}

type CancelOrderCmd struct {
    Reason string `json:"reason"`
}

// ============ Workflow ============

type OrderWorkflow struct{}

func (w *OrderWorkflow) Name() string        { return "OrderWorkflow" }
func (w *OrderWorkflow) SchemaVersion() int  { return 1 }

func (w *OrderWorkflow) Upcast(eventType string, schemaVersion int, rawData map[string]any) map[string]any {
    // Example: Upcast v0 events (no created_at) to v1
    if schemaVersion == 0 && eventType == "order_created" {
        if _, ok := rawData["created_at"]; !ok {
            rawData["created_at"] = time.Now().Format(time.RFC3339)
        }
    }
    return rawData
}

func (w *OrderWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
    var s *OrderState
    if state != nil {
        s = state.(*OrderState)
    }
    
    switch c := cmd.(type) {
    case *CreateOrderCmd:
        if s != nil && s.Status != "" {
            return nil, &model.Rejection{Msg: "order already exists"}
        }
        
        total := 0.0
        for _, item := range c.Items {
            total += item.Price * float64(item.Quantity)
        }
        
        return []model.Event{&OrderCreated{
            OrderID:    c.OrderID,
            CustomerID: c.CustomerID,
            Items:      c.Items,
            Total:      total,
            CreatedAt:  time.Now(),
        }}, nil
        
    case *PayOrderCmd:
        if s == nil {
            return nil, &model.Rejection{Msg: "order not found"}
        }
        if s.Status != "created" {
            return nil, &model.Rejection{Msg: "order cannot be paid in current state"}
        }
        if c.Amount < s.Total {
            return nil, &model.Rejection{Msg: "insufficient payment"}
        }
        
        return []model.Event{&OrderPaid{
            PaymentID: c.PaymentID,
            Amount:    c.Amount,
            PaidAt:    time.Now(),
        }}, nil
        
    case *ShipOrderCmd:
        if s == nil {
            return nil, &model.Rejection{Msg: "order not found"}
        }
        if s.Status != "paid" {
            return nil, &model.Rejection{Msg: "order must be paid before shipping"}
        }
        
        return []model.Event{&OrderShipped{
            TrackingNumber: c.TrackingNumber,
            Carrier:        c.Carrier,
            ShippedAt:      time.Now(),
        }}, nil
        
    case *DeliverOrderCmd:
        if s == nil {
            return nil, &model.Rejection{Msg: "order not found"}
        }
        if s.Status != "shipped" {
            return nil, &model.Rejection{Msg: "order must be shipped before delivery"}
        }
        
        return []model.Event{&OrderDelivered{
            DeliveredAt: time.Now(),
        }}, nil
        
    case *CancelOrderCmd:
        if s == nil {
            return nil, &model.Rejection{Msg: "order not found"}
        }
        if s.Status == "delivered" || s.Status == "cancelled" {
            return nil, &model.Rejection{Msg: "cannot cancel order in current state"}
        }
        
        return []model.Event{&OrderCancelled{
            Reason:     c.Reason,
            CancelledAt: time.Now(),
        }}, nil
    }
    
    return nil, nil
}

func (w *OrderWorkflow) Evolve(state model.State, event model.Event) model.State {
    var s *OrderState
    if state != nil {
        s = state.(*OrderState).Copy().(*OrderState)
    } else {
        s = &OrderState{}
    }
    
    switch e := event.(type) {
    case *OrderCreated:
        s.OrderID = e.OrderID
        s.CustomerID = e.CustomerID
        s.Items = e.Items
        s.Total = e.Total
        s.Status = "created"
        
    case *OrderPaid:
        s.Status = "paid"
        
    case *OrderShipped:
        s.Status = "shipped"
        s.ShippedAt = &e.ShippedAt
        
    case *OrderDelivered:
        s.Status = "delivered"
        t := time.Now()
        s.DeliveredAt = &t
        
    case *OrderCancelled:
        s.Status = "cancelled"
    }
    
    return s
}

func (w *OrderWorkflow) EventToCmd(e model.Event) model.Command {
    // For subscription workflows
    switch e.(type) {
    case *OrderCreated:
        return &CreateOrderCmd{} // Trigger downstream workflows
    }
    return nil
}

func (w *OrderWorkflow) IsFinalEvent(e model.Event) bool {
    switch e.(type) {
    case *OrderDelivered, *OrderCancelled:
        return true
    }
    return false
}
```

---

## HTTP APIs

### Command Gateway

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/commands/{workflow_type}` | Create new workflow |
| POST | `/commands/{workflow_type}/{workflow_id}` | Process command |
| POST | `/commands/{workflow_type}/{workflow_id}/pause` | Pause workflow |
| POST | `/commands/{workflow_type}/{workflow_id}/resume` | Resume workflow |
| POST | `/commands/{workflow_type}/{workflow_id}/cancel` | Cancel workflow |

**Create Workflow:**
```bash
curl -X POST http://localhost:8080/commands/OrderWorkflow \
  -H "Content-Type: application/json" \
  -d '{
    "workflow_id": "order-123",
    "command_type": "create_order",
    "payload": {
      "order_id": "order-123",
      "customer_id": "cust-456",
      "items": [
        {"product_id": "prod-1", "quantity": 2, "price": 29.99}
      ]
    }
  }'
```

**Process Command:**
```bash
curl -X POST http://localhost:8080/commands/OrderWorkflow/order-123 \
  -H "Content-Type: application/json" \
  -d '{
    "command_type": "pay_order",
    "payload": {
      "payment_id": "pay-789",
      "amount": 59.98
    }
  }'
```

### Admin UI API

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/api/workflow-types` | List workflow types |
| GET | `/api/workflows` | List workflows |
| GET | `/api/workflows/{id}` | Get workflow details |
| GET | `/api/workflows/{id}/events` | Get workflow events |
| GET | `/api/activities` | List activities |
| GET | `/api/delays` | List delays |
| GET | `/api/stats` | Dashboard statistics |

---

## Configuration

### Environment Variables

```bash
# Database
export FLEUVE_DATABASE_URL="postgresql://user:pass@host:5432/dbname"

# NATS
export FLEUVE_NATS_URL="nats://localhost:4222"

# Performance
export FLEUVE_MAX_INFLIGHT=10
export FLEUVE_MAX_CACHE_SIZE=10000
export FLEUVE_TRUST_CACHE=false

# Features
export FLEUVE_ENABLE_TRUNCATION=true
export FLEUVE_ENABLE_JETSTREAM=true
export FLEUVE_ENABLE_OTEL=false

# UI
export FLEUVE_UI_TITLE="My Workflows"
export FLEUVE_FRONTEND_DIST="/path/to/ui/dist"
```

### Configuration File (fleuve.toml)

```toml
[fleuve]
database_url = "postgresql://localhost:5432/fleuve"
nats_url = "nats://localhost:4222"
max_inflight = 10
max_cache_size = 10000
enable_truncation = true
enable_jetstream = true
snapshot_interval = 100
ui_title = "Fleuve Workflows"
```

---

## Advanced Topics

### Subscriptions

Workflows can subscribe to events from other workflows:

```go
// Subscribe to order events in analytics workflow
type SubscribeToOrders struct {
    EventTypes  []string `json:"event_types"`
    WorkflowID  string   `json:"workflow_id"`
    Tags        []string `json:"tags,omitempty"`
}

// In Decide:
if cmd, ok := cmd.(*SubscribeToOrders); ok {
    return []model.Event{
        &model.EvSubscriptionAdded{
            Sub: model.Sub{
                EventType:  "order_created",
                WorkflowID: cmd.WorkflowID,
                Tags:       cmd.Tags,
            },
        },
    }, nil
}
```

### Delays and Cron

Schedule future commands:

```go
// Schedule a command for later
type ScheduleFollowUp struct {
    DelayID    string        `json:"delay_id"`
    DelayFor   time.Duration `json:"delay_for"`
    NextCmd    model.Command `json:"next_cmd"`
}

// Cron-based scheduling
type ScheduleReport struct {
    DelayID  string `json:"delay_id"`
    CronExpr string `json:"cron_expr"` // "0 9 * * 1" = Every Monday 9am
    Timezone string `json:"timezone"`
    NextCmd  model.Command `json:"next_cmd"`
}
```

### Activities (Side Effects)

Execute external operations with retries:

```go
type OrderAdapter struct{}

func (a *OrderAdapter) ActOn(ctx context.Context, event *model.ConsumedEvent, ac *model.ActionContext) (<-chan model.ActionYield, error) {
    ch := make(chan model.ActionYield)
    
    go func() {
        defer close(ch)
        
        switch e := event.Event.(type) {
        case *OrderCreated:
            // Call payment gateway
            paymentID, err := a.processPayment(ctx, e)
            if err != nil {
                // Will be retried based on retry policy
                return
            }
            
            // Emit command to pay order
            ch <- model.CommandYield{Cmd: &PayOrderCmd{
                PaymentID: paymentID,
                Amount:    e.Total,
            }}
        }
    }()
    
    return ch, nil
}

func (a *OrderAdapter) ToBeActOn(event *model.ConsumedEvent) bool {
    switch event.Event.(type) {
    case *OrderCreated:
        return true
    }
    return false
}
```

### Snapshots

Optimize state reconstruction:

```go
// Repository automatically creates snapshots every N events
repo := repo.NewPGXRepo(
    pool,
    "OrderWorkflow",
    workflow,
    storage,
    repo.WithPGXSnapshotInterval(100), // Snapshot every 100 events
)

// State reconstruction uses snapshot + replay
state, err := repo.GetCurrentState(ctx, workflowID)
// If snapshot exists at v500, only replays events 501-current
```

---

## Production Deployment

### Docker Compose

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: fleuve
      POSTGRES_USER: fleuve
      POSTGRES_PASSWORD: secret
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

  nats:
    image: nats:2.9-alpine
    command: ["--jetstream", "--store_dir", "/data"]
    volumes:
      - nats_data:/data
    ports:
      - "4222:4222"

  fleuve-runner:
    build: .
    command: ./fleuve-runner -type OrderWorkflow
    environment:
      FLEUVE_DATABASE_URL: postgresql://fleuve:secret@postgres:5432/fleuve
      FLEUVE_NATS_URL: nats://nats:4222
    depends_on:
      - postgres
      - nats

  fleuve-gateway:
    build: .
    command: ./fleuve-gateway -addr :8080
    environment:
      FLEUVE_DATABASE_URL: postgresql://fleuve:secret@postgres:5432/fleuve
      FLEUVE_NATS_URL: nats://nats:4222
    ports:
      - "8080:8080"
    depends_on:
      - postgres

  fleuve-ui:
    build: .
    command: ./fleuve-ui -addr :3000
    environment:
      FLEUVE_DATABASE_URL: postgresql://fleuve:secret@postgres:5432/fleuve
      FLEUVE_UI_TITLE: "Order Management"
    ports:
      - "3000:3000"
    depends_on:
      - postgres

volumes:
  postgres_data:
  nats_data:
```

### Health Checks

```go
// In your main.go
http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
    // Check database
    ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
    defer cancel()
    
    if err := pool.Ping(ctx); err != nil {
        http.Error(w, "database unhealthy", 503)
        return
    }
    
    // Check NATS
    if nc.Status() != nats.CONNECTED {
        http.Error(w, "nats unhealthy", 503)
        return
    }
    
    w.WriteHeader(200)
    json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
})
```

### Metrics

```go
import "github.com/doomervibe/fleuve-go/pkg/metrics"

m := metrics.NewFleuveMetrics()

// Record events
m.RecordEventProcessed()

// Record commands
start := time.Now()
// ... process command
m.RecordCommandProcessed(time.Since(start))

// Get snapshot
snapshot := m.Snapshot()
log.Printf("Events processed: %d", snapshot.EventsProcessedTotal)
```

---

## API Reference

### Package: model

```go
// Core interfaces
type Workflow interface { ... }
type State interface { ... }
type Event interface { ... }
type Adapter interface { ... }

// Lifecycle
type LifecycleState string
const (
    LifecycleActive   LifecycleState = "active"
    LifecyclePaused   LifecycleState = "paused"
    LifecycleCanceled LifecycleState = "cancelled"
)

// Error types
type Rejection struct {
    Msg string
}

type AlreadyExists struct {
    Rejection
}

type WorkflowNotFound struct {
    ID           string
    WorkflowType string
}
```

### Package: repo

```go
// SQL-backed repository (database/sql)
type Repo struct { ... }

func (r *Repo) CreateNew(ctx context.Context, cmd Command, id string, tags []string) (*StoredState, error)
func (r *Repo) ProcessCommand(ctx context.Context, id string, cmd Command) (*StoredState, []Event, *Rejection)
func (r *Repo) PauseWorkflow(ctx context.Context, id string, reason string) (*StoredState, *Rejection)
func (r *Repo) ResumeWorkflow(ctx context.Context, id string) (*StoredState, *Rejection)
func (r *Repo) CancelWorkflow(ctx context.Context, id string, reason string) (*StoredState, *Rejection)
func (r *Repo) GetCurrentState(ctx context.Context, id string) (*StoredState, error)

// Ephemeral storage
type InProcessEphemeralStorage struct { ... }
type TieredEphemeralStorage struct { ... }

// PostgreSQL pool
func NewPGXPool(ctx context.Context, databaseURL string, maxConns int32) (*pgxpool.Pool, error)
```

### Package: stream

```go
// Stream reader
type PGReader struct { ... }
func NewPGReader(db *sql.DB, readerName string, batchSize int) *PGReader
func (r *PGReader) IterEvents(ctx context.Context) <-chan *ConsumedEvent

// NATS reader
type NATSReader struct { ... }
func NewNATSReader(natsURL, workflowType string, opts ...NATSReaderOption) (*NATSReader, error)
```

### Package: actions

```go
// Action executor
type ActionExecutor struct { ... }

func NewActionExecutor(adapter model.Adapter, repo Repository, opts ...ActionExecutorOption) *ActionExecutor
func (e *ActionExecutor) Start(ctx context.Context) error
func (e *ActionExecutor) Stop() error
func (e *ActionExecutor) ExecuteAction(ctx context.Context, event *model.ConsumedEvent) error
```

### Package: gateway

```go
type FleuveCommandGateway struct { ... }

func NewFleuveCommandGateway() *FleuveCommandGateway
func (g *FleuveCommandGateway) RegisterWorkflowType(workflowType string, repo Repository, parser CommandParser)
func (g *FleuveCommandGateway) SetActionExecutor(executor *actions.ActionExecutor)
func (g *FleuveCommandGateway) RegisterRoutes(mux *http.ServeMux)
```

---

## Examples

See the `examples/` directory for complete, runnable examples:

- **Counter**: Simple counter workflow
- **Order Processing**: E-commerce order workflow with payments, shipping
- **Subscription**: Cross-workflow subscriptions
- **Delayed Tasks**: Scheduled command execution
- **Activities**: External API calls with retries

---

## Migration from Python

See [INTEGRATION.md](./INTEGRATION.md) for connecting to an existing database and [behavior-and-python-parity.md](./behavior-and-python-parity.md) for ordering, offsets, and recovery.

Key points:
- Go uses the same PostgreSQL schema and the same HTTP/NATS wire shapes as Python.
- **Python defines the intended runtime behavior;** align Go when they differ.
- **Do not run Python and Go runners concurrently** on the same consumer/stream; migrate with **cutover**, not mixed runners.
