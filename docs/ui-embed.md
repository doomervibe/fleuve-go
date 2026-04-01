# Bundled admin UI (`pkg/uiembed` + `pkg/uibackend`)

The Python Fleuve Vite **`frontend_dist`** is vendored under **`pkg/uiembed/dist/`** and embedded at compile time so operators get the same console **without Node.js** at runtime.

The read API is **`pkg/uibackend`** (library); **`examples/ui_server`** is a thin reference that composes API + static files. You can also mount **`NewHandler`** and **`uiembed.NewHandler`** on your own `http.ServeMux`.

---

## What is embedded

| Path | Role |
|------|------|
| [`pkg/uiembed/embed.go`](../pkg/uiembed/embed.go) | `//go:embed dist`, `IndexHTML`, `DistFS` |
| [`pkg/uiembed/handler.go`](../pkg/uiembed/handler.go) | `/assets/*`, optional root files, SPA fallback |
| [`pkg/uiembed/dist/`](../pkg/uiembed/dist/) | `index.html`, hashed assets under `assets/` |

[`pkg/uibackend`](../pkg/uibackend/handler.go) implements the `/api/*` JSON contract (and `GET /health`). Use **`uibackend.NewCombinedHandler`** to mirror the Python single-app layout.

---

## Refreshing the bundle

Default source is **`../les/fleuve/ui/frontend_dist`** (sibling checkout). Override with **`FLEUVE_UI_DIST`**:

```bash
./scripts/vendor-fleuve-ui.sh
go build -o fleuve-ui ./examples/ui_server
```

---

## API contract

The SPA expects JSON shapes compatible with Python Fleuve’s admin API (non-`null` arrays for lists). Route overview: [http-api.md](./http-api.md). Deeper engine parity: [python-go-parity-checklist.md](./python-go-parity-checklist.md).
