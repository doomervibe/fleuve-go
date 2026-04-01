# Python ↔ Go Fleuve parity checklist

Canonical comparison of **core engine behavior** between Python Fleuve (`les` / `anomaly/fleuve`) and **fleuve-go**. Update rows when either side changes.

Legend: **match** — same semantics; **partial** — close or configurable; **gap** — not aligned or not applicable.

| Area | Python behavior | Go behavior | Status | Notes |
|------|-----------------|-------------|--------|-------|
| **Repo / optimistic concurrency** | Unique `(workflow_id, workflow_version)` on append | Same constraint in `stored_events` | match | `migrations/001_initial_schema.up.sql` |
| **Create / command / lifecycle** | `process_command`, pause/resume/cancel, `continue_as_new`, replay | `pkg/repo` `ProcessCommand`, `CreateNew`, pause/resume/cancel, replay paths | partial | Confirm each command type string per workflow |
| **Snapshots & truncation** | Snapshot gating, cache / `trust_cache` | `WithSnapshotInterval`, `trust_cache` on `Repo` | partial | Align intervals and truncation flags with ops runbooks |
| **Finality** | `IsFinalEvent` / terminal events | `model` finality on events | partial | Per–workflow-type audit |
| **load_state / replay cursor** | Replay after snapshot version | `pkg/repo` loads events with correct cursor after snapshot | match | Go fixed off-by-one vs snapshot `version` (events strictly after snapshot) |
| **Sync: subscriptions** | Upsert/delete fan-out rows | Repo sync handlers + `subscriptions` table | partial | **ON CONFLICT** columns must match schema in both codepaths |
| **Delays: one-shot** | Scheduler fires `delay_complete` | `pkg/delay` inserts completion event / schedule rows | partial | Compare `delay_schedules` lifecycle and orphan cleanup |
| **Delays: cron** | `croniter` + timezone for next fires | `robfig/cron` in UI; scheduler in `pkg/delay` | partial | Expression sets and DST edge cases may differ |
| **Delays: orphan cleanup** | Delete schedules when workflow missing; commit semantics | Go scheduler NULL `MAX(workflow_version)` handling + commit | match | Verify after workflow delete / truncate scenarios |
| **Stream / PG reader** | `StreamReader` offset semantics | `pkg/stream` PG reader | partial | Document offset units and exclusive/inclusive boundaries |
| **JetStream** | Publisher + consumer patterns | `pkg/jetstream` | partial | Advisory lock held across poll cycles in Go — align ops checklist |
| **Hybrid reader / NATS payload** | Optional hybrid / header conventions | PG-first in typical Go deploy | gap | Track cross-language NATS body/header parity separately |
| **Actions** | Activity rows, retries, cancel | `pkg/actions` + `workflow_activities` | partial | Nullable columns, `event_type` migration `002_workflow_activities_event_type` |
| **Command gateway** | `FleuveCommandGateway` routes, e.g. create may require `workflow_id` | `pkg/gateway` | partial | Intentional HTTP differences should be listed in gateway docs |
| **Runner: rate limit** | Token bucket / max EPS | `MaxEventsPerSecond` etc. in `pkg/config` / runner | partial | Tune to match production Python settings |
| **Scaling** | `scaling_operations`, `target_offset` drain | `pkg/scaling` + table in migrations | partial | Same table; verify drain semantics |
| **Partitioning** | MD5 hex → int % N | `pkg/partitioning` | match | Same algorithm as Python `partitioning.py` |
| **Reader naming** | `{type}_runner_partition_{i}_of_{n}` | Same pattern in Go stream config | match | Needed for multi-partition cutover |
| **Resilience** | Context cancel, graceful stop | Runner / publisher `Stop()`, context | partial | Audit each long-running loop |
| **Outbox** | Publish + mark `published` | Repo outbox + publisher | partial | `outbox_batch_size`, poll interval parity |
| **UI read API** | FastAPI `fleuve.ui.backend.api` | `pkg/uibackend` library + `examples/ui_server` or `fleuve-gateway -with-ui` | partial | **501** batch endpoints match Python; optional `StateResolver` + per-type `Replay` in [`pkg/uibackend`](../pkg/uibackend/options.go) for typed state (else latest-event JSON like Python without models) |
| **UI static** | `frontend_dist` + title placeholder | `pkg/uiembed` embed + `FLEUVE_UI_TITLE` / cwd title | match | Run `./scripts/vendor-fleuve-ui.sh` after upstream UI changes |

---

## Maintenance

When changing Python or Go behavior, update the relevant row and link to the function or file (e.g. `pkg/repo/repo.go`, `les/fleuve/runner.py`).

## Related docs

- [behavior-and-python-parity.md](./behavior-and-python-parity.md) — runner cutover and single-writer rules
- [http-api.md](./http-api.md) — HTTP surfaces
- [ui-embed.md](./ui-embed.md) — vendored UI and refresh script
