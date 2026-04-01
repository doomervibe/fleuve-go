# HTTP API reference

HTTP surfaces in this repository:

1. **Command gateway** (`fleuve-gateway`) — mutates workflow state via commands.  
2. **Admin UI** — JSON under `/api/*` + embedded SPA from [`pkg/uibackend`](../pkg/uibackend/) + [`pkg/uiembed`](../pkg/uiembed/): either the **`examples/ui_server`** reference binary or **`fleuve-gateway -with-ui`** (same port as commands).

Both use Go 1.22+ **`http.ServeMux` patterns** with path wildcards (`{id}`, `{full_path...}`).

---

## Command gateway

Base URL shown as `http://localhost:8080`. All routes are **POST** and expect **`Content-Type: application/json`**.

| Route | Description |
|-------|-------------|
| `/commands/{workflow_type}` | Create a new workflow instance |
| `/commands/{workflow_type}/{workflow_id}` | Apply a command to an existing instance |
| `/commands/{workflow_type}/{workflow_id}/pause` | Pause lifecycle |
| `/commands/{workflow_type}/{workflow_id}/resume` | Resume lifecycle |
| `/commands/{workflow_type}/{workflow_id}/cancel` | Cancel lifecycle |
| `/commands/{workflow_type}/{workflow_id}/retry/{event_number}` | Retry a failed activity (requires registered workflow model) |

### Create workflow

**Path:** `POST /commands/{workflow_type}`

```json
{
  "workflow_id": "string (required)",
  "command_type": "string",
  "payload": { }
}
```

Response shape: [`CommandResponse`](../pkg/gateway/gateway.go) (`status`, `workflow_id`, `version`, `events_count`, `message`). Errors are JSON with a `detail` field where applicable.

### Process command

**Path:** `POST /commands/{workflow_type}/{workflow_id}`

```json
{
  "command_type": "string",
  "payload": { }
}
```

### CounterWorkflow command types

| `command_type` | `payload` |
|----------------|-----------|
| `increment` | `{ "amount": <number> }` |
| `reset` | `{}` or omitted |

Defined by [`counterworkflow.ParseGatewayCommand`](../pkg/counterworkflow/parse.go).

---

## Admin UI API

Served from **`fleuve-ui`** (default port **3000**). Responses use JSON; list endpoints return **arrays** (`[]`), not `null`, for compatibility with the vendored React app.

### Health & shell

| Method | Route | Description |
|--------|-------|-------------|
| GET | `/health` | Liveness |
| GET | `/` | SPA shell (when static UI enabled) or API root |

### Workflows

| Method | Route | Query parameters |
|--------|-------|------------------|
| GET | `/api/workflow-types` | — |
| GET | `/api/workflows` | `workflow_type`, `search`, `limit` (default 100, max 1000), `offset` |
| GET | `/api/workflows/{workflow_id}` | — |
| GET | `/api/workflows/{workflow_id}/events` | — |
| GET | `/api/workflows/{workflow_id}/state/{version}` | — |
| GET | `/api/workflows/{workflow_id}/state-diff/{v1}/{v2}` | — |
| GET | `/api/workflows/{workflow_id}/activities` | — |
| GET | `/api/workflows/{workflow_id}/delays` | — |

### Global lists

| Method | Route | Query parameters |
|--------|-------|------------------|
| GET | `/api/events` | `workflow_type`, `workflow_id`, `event_type`, `limit`, `offset` |
| GET | `/api/events/{event_id}` | `event_id` is `global_id` |
| GET | `/api/activities` | `workflow_id`, `workflow_type`, `status`, `limit`, `offset` |
| GET | `/api/delays` | `workflow_type`, `workflow_id`, `limit`, `offset` |
| GET | `/api/stats` | — |

### Batch (limited)

| Method | Route | Notes |
|--------|-------|-------|
| POST | `/api/workflows/batch/cancel` | Documented as requiring gateway per handler |
| POST | `/api/workflows/batch/replay` | May return **501 Not Implemented** |

### Static assets

| Method | Route | Description |
|--------|-------|-------------|
| GET | `/assets/…` | Hashed JS/CSS from vendored build |
| GET | `/{full_path...}` | SPA fallback for client-side routes |

---

## CORS

When the UI runs **without** embedded static files (`-api-only` or no dist), requests are wrapped with permissive CORS headers for local tooling. **Do not** expose that mode to untrusted browsers on the public internet without hardening.

---

## See also

- [Getting started](./getting-started.md) — curl examples  
- [Architecture](./architecture.md) — where gateway and UI sit in the system  
