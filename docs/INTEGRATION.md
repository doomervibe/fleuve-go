# Integrating Go port with an existing Python database

The Go port is **wire-compatible** with the Python implementation’s PostgreSQL schema. For **ordering, delivery, offsets, and recovery**, the **Python Fleuve implementation is the behavioral reference**; see [behavior-and-python-parity.md](./behavior-and-python-parity.md).

## Not supported: mixed Python + Go runners

Running **Python and Go runners at the same time** against the **same consumer / partition / workflow stream** is **not supported** by this package and is not a design goal. Use **cutover**: stop Python runners for a scope, then start Go (or the reverse) after validation.

Read-only use of the **same database** (e.g. `fleuve-ui`, reporting) while one side owns processing is fine.

## Option 1: Direct connection (same schema)

Point Go at a database already migrated by Python:

```bash
export FLEUVE_DATABASE_URL="postgresql://user:pass@host:5432/your_existing_db"
export FLEUVE_NATS_URL="nats://your-nats:4222"

./fleuve-runner -type CounterWorkflow
./fleuve-gateway -addr :8080
./fleuve-ui -addr :3000
# optional: ./fleuve-ui -frontend /path/to/fleuve/ui/frontend_dist
```

**No new migrations from Go** are required if tables already exist from Python’s Alembic migrations.

## Option 2: Verify schema compatibility

```sql
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public'
AND table_name IN (
    'stored_events', 'offsets', 'subscriptions', 'external_subscriptions',
    'activities', 'delay_schedules', 'snapshots', 'scaling_operations',
    'workflow_metadata', 'workflow_search_attributes', 'outbox'
);

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'stored_events'
ORDER BY ordinal_position;
```

## Schema (expected match with Python)

| Table | Notes |
|-------|--------|
| `stored_events` | Same columns and uniqueness as Python |
| `activities` | Same status values as Python |
| `delay_schedules` | Same cron / delay fields |
| `snapshots` | Same JSONB state |
| `offsets` | Reader keys must agree with Python for shared consumers—**only one active runner stack** should advance a given reader |

## If the schema is outdated

```bash
# In the Python repo
alembic upgrade head

export FLEUVE_DATABASE_URL="postgresql://..."
./fleuve-runner -type CounterWorkflow
```

## Connection pool (production)

```go
pool, err := repo.NewPGXPool(ctx, databaseURL, 25)
```

Pool defaults include health checks and connection lifetime limits (see `pkg/repo`).

## Migration checklist (cutover, not concurrent processing)

1. Validate schema (queries above).
2. Read [behavior-and-python-parity.md](./behavior-and-python-parity.md) for offsets and recovery.
3. **Stop** Python runners for the workflows you are moving.
4. Ensure **`offsets`** / JetStream consumer state is consistent with where Python stopped (or accept reprocessing per parity doc).
5. Start Go runners and gateway; validate with tests and UI.
6. Do **not** run Python and Go runners for the **same** stream scope in parallel.
