# Fleuve Go Examples

This directory contains comprehensive examples demonstrating Fleuve's capabilities.

## Table of Contents

1. [Counter Example](#counter-example) - Basic workflow with state evolution
2. [Order Processing Example](#order-processing-example) - E-commerce workflow with external adapters
3. [Subscription Example](#subscription-example) - Cross-workflow event subscriptions
4. [Running Examples](#running-examples)
5. [Database Setup](#database-setup)
6. [Verifying Results](#verifying-results)

---

## Counter Example

**Location:** `examples/counter/main.go`

A simple counter workflow demonstrating:
- Basic command processing (Increment, Reset)
- State evolution with event sourcing
- Concurrent command handling
- Lifecycle management

### Running the Counter Example

```bash
# From the fleuve-go root directory
go run ./examples/counter/main.go
```

### Expected Output

```
=== Creating new counter workflow ===
Created workflow: ID=counter-001, Version=1
State: {
  "count": 5,
  "lifecycle": "active"
}
Version: 1

=== Incrementing counter by 10 ===
Processed command: Version=2, Events=1
State: {
  "count": 15,
  "lifecycle": "active"
}
Version: 2

✅ Example completed successfully!
```

### What It Demonstrates

1. **Command Processing**: Commands are processed and validated
2. **Event Generation**: Commands produce events (CounterIncremented, CounterReset)
3. **State Evolution**: State is updated by applying events
4. **Concurrency**: Multiple goroutines can process commands concurrently
5. **Idempotency**: Cache ensures consistent state retrieval

---

## Order Processing Example

**Location:** `examples/order/main.go`

A complete e-commerce order workflow demonstrating production-ready patterns:
- **State Machine**: created → paid → shipped → delivered (or cancelled)
- **External Adapters**: Mock payment gateway with HTTP API
- **Retry Logic**: Exponential backoff for failed operations
- **Error Handling**: Comprehensive error handling and compensation
- **DLQ Behavior**: Dead Letter Queue after max retries
- **Compensation**: Automatic refunds on cancellation

### Running the Order Example

```bash
# From the fleuve-go root directory
go run ./examples/order/main.go
```

### Expected Output

```
=== E-Commerce Order Workflow Example ===
This example demonstrates:
  ✓ State machine transitions
  ✓ External payment gateway integration
  ✓ Retry with exponential backoff
  ✓ Error handling and DLQ behavior
  ✓ Compensation (refunds)

=== Example 1: Successful Order Flow ===
✓ Order created: ID=ORD-1234567890, Status=created
✓ Payment processed: PaymentID=PAY-ORD-1234567890-1234567890
✓ Order shipped: TrackingNumber=TRACK-12345
✓ Order delivered: Status=delivered
✅ Order workflow completed successfully!

=== Example 2: Order with Payment Failures (DLQ) ===
✓ Order created: ID=ORD-1234567890-FAIL, Status=created
✓ Order moved to DLQ after 3 failed attempts
  Reason: payment_failed_after_3_attempts: payment_gateway_timeout
✅ DLQ behavior demonstrated successfully!

=== Example 3: Order Cancellation with Refund ===
✓ Order created: ID=ORD-1234567890-CANCEL, Status=created
✓ Payment processed: PaymentID=PAY-ORD-1234567890-CANCEL-1234567890
✓ Order cancelled: Reason=customer_request
✓ Refund triggered for PaymentID=PAY-ORD-1234567890-CANCEL-1234567890
✅ Cancellation with compensation demonstrated!
```

### What It Demonstrates

1. **State Machine Transitions**
   - Order lifecycle: created → paid → shipped → delivered
   - Invalid transitions are rejected (e.g., can't ship unpaid order)

2. **External Adapter Integration**
   - Mock HTTP payment server on port 8090
   - Payment gateway with configurable failure rate
   - Realistic API simulation

3. **Retry with Exponential Backoff**
   - Failed payments are retried automatically
   - Backoff: 100ms → 200ms → 400ms → 800ms
   - Jitter added to prevent thundering herd

4. **DLQ (Dead Letter Queue)**
   - After 3 failed attempts, order moves to cancelled state
   - Error information preserved for debugging
   - Can be reprocessed later with manual intervention

5. **Compensation Logic**
   - Cancellation triggers refund if payment was processed
   - Automatic compensation for paid orders
   - Clean state management

---

## Subscription Example

**Location:** `examples/subscription/main.go` (placeholder)

Cross-workflow event subscription demonstrating:
- Multiple workflows subscribing to same event
- Tag-based routing and filtering
- Event fan-out to multiple subscribers
- Subscription lifecycle management

### Concepts Demonstrated

1. **EvSubscriptionAdded Event**
   - Workflows explicitly subscribe to event types
   - Subscription metadata stored in workflow state

2. **Tag-Based Routing**
   - Events can be tagged (e.g., ["priority", "vip"])
   - Subscribers filter by tags using `Tags` (any match) or `TagsAll` (all must match)

3. **Event Fan-Out**
   - Single event can trigger multiple workflow instances
   - Each subscriber processes independently

4. **Subscription Matching**
   - `Tags`: Match if any tag is present (OR logic)
   - `TagsAll`: Match only if all tags are present (AND logic)

---

## Running Examples

### Prerequisites

1. **PostgreSQL Database**
   ```bash
   # Using Docker
   docker run -d \
     --name fleuve-postgres \
     -e POSTGRES_DB=fleuve \
     -e POSTGRES_USER=fleuve \
     -e POSTGRES_PASSWORD=secret \
     -p 5432:5432 \
     postgres:15
   
   # Or use docker-compose
   docker-compose up -d postgres
   ```

2. **Database Migrations**
   ```bash
   # Apply migrations
   migrate -database "postgresql://fleuve:secret@localhost:5432/fleuve?sslmode=disable" \
     -path migrations up
   ```

3. **Environment Variables**
   ```bash
   export FLEUVE_DATABASE_URL="postgresql://fleuve:secret@localhost:5432/fleuve?sslmode=disable"
   export FLEUVE_NATS_URL="nats://localhost:4222"  # Optional
   ```

### Running with Docker Compose

```bash
# Start all services (postgres, nats, runners)
docker-compose up -d

# View logs
docker-compose logs -f fleuve-runner

# Stop services
docker-compose down
```

### Running Individual Examples

```bash
# Counter example
go run ./examples/counter/main.go

# Order example
go run ./examples/order/main.go

# Subscription example
go run ./examples/subscription/main.go
```

---

## Database Setup

### Option 1: Local PostgreSQL

```bash
# Install PostgreSQL (macOS)
brew install postgresql@15
brew services start postgresql@15

# Create database
createdb fleuve

# Run migrations
migrate -database "postgresql://localhost:5432/fleuve?sslmode=disable" \
  -path migrations up
```

### Option 2: Docker

```bash
# Start PostgreSQL container
docker run -d \
  --name fleuve-postgres \
  -e POSTGRES_DB=fleuve \
  -e POSTGRES_USER=fleuve \
  -e POSTGRES_PASSWORD=secret \
  -p 5432:5432 \
  postgres:15

# Run migrations
migrate -database "postgresql://fleuve:secret@localhost:5432/fleuve?sslmode=disable" \
  -path migrations up
```

### Option 3: Docker Compose (Recommended)

```bash
# Start all services
docker-compose up -d postgres

# Initialize database
docker-compose exec postgres psql -U fleuve -d fleuve -f /docker-entrypoint-initdb.d/init.sql
```

---

## Verifying Results

### Query Workflow State

```bash
# Connect to database
psql -h localhost -U fleuve -d fleuve

# Query all workflows
SELECT workflow_id, workflow_type, version, created_at 
FROM workflows 
ORDER BY created_at DESC 
LIMIT 10;

# Query events for a specific workflow
SELECT global_id, event_type, version, created_at 
FROM events 
WHERE workflow_id = 'ORD-1234567890'
ORDER BY version;

# Query current state
SELECT workflow_id, state 
FROM workflows 
WHERE workflow_id = 'ORD-1234567890';
```

### Using the HTTP API

```bash
# Get workflow state
curl http://localhost:8080/api/workflows/ORD-1234567890

# Get workflow events
curl http://localhost:8080/api/workflows/ORD-1234567890/events

# Create new order
curl -X POST http://localhost:8080/api/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "workflow_type": "OrderWorkflow",
    "workflow_id": "ORD-999",
    "command": {
      "type": "CreateOrder",
      "order_id": "ORD-999",
      "customer_id": "CUST-001",
      "amount": 99.99,
      "currency": "USD"
    }
  }'
```

### Using the UI

The framework embeds the Python Fleuve **`frontend_dist`** under `pkg/uiembed/dist` (no Node at runtime). Re-vendor after UI changes with `./scripts/vendor-fleuve-ui.sh /path/to/fleuve/ui/frontend_dist`.

1. Start the UI server:
   ```bash
   docker-compose up -d fleuve-ui
   # or locally:
   go run ./cmd/ui -addr :3000
   ```

2. Open the browser: `http://localhost:3000`

3. Use **Dashboard** and **Workflows** in the nav to inspect stats and workflow details.

---

## Common Patterns

### Pattern 1: Event Sourcing

Every state change is captured as an immutable event:

```go
// Command creates event(s)
events, rejection := workflow.Decide(state, cmd)

// Events are persisted
for _, event := range events {
    // Save to event store
    saveEvent(event)
    
    // Evolve state
    state = workflow.Evolve(state, event)
}
```

### Pattern 2: External Adapter

External systems are called from commands:

```go
func (w *OrderWorkflow) Decide(state model.State, cmd model.Command) ([]model.Event, *model.Rejection) {
    switch c := cmd.(type) {
    case *ProcessPayment:
        // Call external payment gateway
        result := w.paymentGateway.ProcessPayment(c.OrderID, s.Amount, s.Currency)
        
        if result.Success {
            return []model.Event{&PaymentProcessed{...}}, nil
        } else {
            return []model.Event{&PaymentFailed{...}}, nil
        }
    }
}
```

### Pattern 3: Retry with Backoff

Failed operations are retried with exponential backoff:

```go
func (pg *PaymentGateway) calculateBackoff(attempt int) time.Duration {
    delay := 100 * time.Millisecond
    for i := 0; i < attempt; i++ {
        delay *= 2  // Exponential
    }
    jitter := time.Duration(rand.Float64() * 0.2 * float64(delay))
    return delay + jitter
}
```

### Pattern 4: Compensation

Compensating actions undo previous operations:

```go
case *OrderCancelled:
    s.Status = OrderStatusCancelled
    if e.RefundProcessed {
        // Trigger refund
        pg.ProcessRefund(s.OrderID, s.PaymentID)
    }
```

---

## Troubleshooting

### Issue: Database Connection Failed

```bash
# Check PostgreSQL is running
docker ps | grep postgres

# Test connection
psql -h localhost -U fleuve -d fleuve -c "SELECT 1"

# Check logs
docker logs fleuve-postgres
```

### Issue: Migrations Failed

```bash
# Check migration status
migrate -database "postgresql://fleuve:secret@localhost:5432/fleuve?sslmode=disable" \
  -path migrations version

# Force version
migrate -database "..." -path migrations force 1
```

### Issue: Port Already in Use

```bash
# Find process using port
lsof -i :5432
lsof -i :8080
lsof -i :8090

# Kill process
kill -9 <PID>
```

---

## Performance Targets

When running benchmarks:

```bash
go test -bench=. -benchmem ./pkg/...
```

Expected performance:
- **Command Processing**: >5,000 cmd/sec
- **State Evolution**: >50,000 evolve/sec
- **Cache Operations**: >100,000 ops/sec
- **Event Throughput**: >10,000 events/sec

---

## Next Steps

1. **Explore the Code**: Read through example implementations
2. **Modify Examples**: Try changing business logic
3. **Add Metrics**: Integrate Prometheus metrics
4. **Add Tracing**: Integrate OpenTelemetry tracing
5. **Write Tests**: Add unit and integration tests
6. **Deploy**: Use Docker Compose for production-like setup

For more information, see:
- [Main README](../README.md)
- [Integration Guide](../docs/INTEGRATION.md)
- [API Documentation](../docs/API.md)

This directory contains complete, runnable examples demonstrating Fleuve Go usage.

## Prerequisites

1. PostgreSQL 13+ running
2. Go 1.21+

## Setup

```bash
# Set database URL
export FLEUVE_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/fleuve?sslmode=disable"

# Create database (if needed)
createdb fleuve

# Run migrations (or use existing Python DB)
psql -d fleuve -f ../../migrations/001_initial_schema.up.sql
```

## Examples

### 1. Counter (`./counter`)

Simple counter workflow demonstrating:
- Creating workflows
- Processing commands
- State evolution
- Concurrent safety

```bash
cd counter
go run main.go
```

### 2. Order Processing (`./order`)

E-commerce order workflow with:
- Multiple event types
- State transitions
- External adapter
- Activity execution

```bash
cd order
go run main.go
```

### 3. Subscription (`./subscription`)

Cross-workflow subscriptions:
- Subscribe to events from other workflows
- Tag-based filtering
- Event routing

```bash
cd subscription
go run main.go
```

## Testing with Existing Python Database

If you have an existing Python Fleuve database:

```bash
# Point to existing DB
export FLEUVE_DATABASE_URL="postgresql://user:pass@host:5432/existing_fleuve"

# Run Go code against it
cd counter
go run main.go
```

The Go implementation uses the same schema and can read/write to the same database.
