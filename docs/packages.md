# Package layout

Source under [`pkg/`](../pkg/) is the library surface of Fleuve Go. Commands under [`cmd/`](../cmd/) are thin `main` packages that wire config, NATS, and HTTP.

---

## Core domain

| Package | Responsibility |
|---------|------------------|
| [`model`](../pkg/model/) | `Workflow`, `State`, `Event`, `Command`, lifecycle, consumed-event types |
| [`counterworkflow`](../pkg/counterworkflow/) | Built-in **CounterWorkflow** aggregate + HTTP command parsing |

---

## Persistence

| Package | Responsibility |
|---------|------------------|
| [`repo`](../pkg/repo/) | `PGXRepo`, snapshots, JetStream post-commit hook, ephemeral cache |
| [`postgres`](../pkg/postgres/) | SQL-oriented structs / helpers aligned with schema |

---

## Ingress & admin

| Package | Responsibility |
|---------|------------------|
| [`gateway`](../pkg/gateway/) | REST command gateway, request/response types |
| [`uibackend`](../pkg/uibackend/) | Admin JSON API, static asset handling, PG session abstraction |
| [`uiembed`](../pkg/uiembed/) | `go:embed` of vendored `frontend_dist` |
| [`fleuvecmd`](../pkg/fleuvecmd/) | Registers CounterWorkflow on gateway/runner with shared repo options |

---

## Stream processing

| Package | Responsibility |
|---------|------------------|
| [`stream`](../pkg/stream/) | PostgreSQL reader, NATS JetStream reader, publish-after-commit helpers |
| [`runner`](../pkg/runner/) | Main consumer loop, inflight / offset coordination |
| [`actions`](../pkg/actions/) | Activity executor, retries, DB persistence hooks |

---

## Scheduling & scale

| Package | Responsibility |
|---------|------------------|
| [`delay`](../pkg/delay/) | Delay schedules, cron helpers |
| [`scaling`](../pkg/scaling/) | Partition scaling operations |

---

## Observability & utilities

| Package | Responsibility |
|---------|------------------|
| [`config`](../pkg/config/) | `fleuve.toml` + `FLEUVE_*` loading |
| [`tracing`](../pkg/tracing/) | OpenTelemetry bootstrap |
| [`metrics`](../pkg/metrics/) | Prometheus-oriented metrics |
| [`external`](../pkg/external/) | External NATS messaging integration |
| [`truncation`](../pkg/truncation/) | Event truncation service |

---

## Testing

| Package | Responsibility |
|---------|------------------|
| [`testing`](../pkg/testing/) | Shared test helpers where present |

When adding a new workflow type, you typically touch **`model`** (or a domain package), **`repo`** wiring, **`gateway`** registration, and **`cmd/runner`** selection—mirroring how **CounterWorkflow** is integrated.
