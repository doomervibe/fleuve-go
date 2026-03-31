# Bundled admin UI (`pkg/uiembed`)

The **`fleuve-ui`** binary serves a **vendored build** of the Python Fleuve Vite app so operators get the same console **without Node.js** at runtime.

---

## What is embedded

| Path | Role |
|------|------|
| [`pkg/uiembed/embed.go`](../pkg/uiembed/embed.go) | `//go:embed all:dist` + `fs.Sub` → exported `Dist` |
| [`pkg/uiembed/dist/`](../pkg/uiembed/dist/) | Production `frontend_dist`: `index.html`, hashed assets under `assets/` |

[`pkg/uibackend`](../pkg/uibackend/api.go) mounts `uiembed.Dist` when no `-frontend` / `FLEUVE_FRONTEND_DIST` override is set and `-api-only` is false.

---

## Refreshing the bundle

When the upstream React app changes, copy a fresh build:

```bash
./scripts/vendor-fleuve-ui.sh /path/to/fleuve/ui/frontend_dist
```

Or set `FLEUVE_PYTHON_UI_DIST` to that directory and run the script with no args (see script header). Then rebuild:

```bash
go build -o fleuve-ui ./cmd/ui
```

---

## Overrides

| Mechanism | Effect |
|-----------|--------|
| `-frontend /dir` | Serve files from disk instead of embed |
| `FLEUVE_FRONTEND_DIST` | Same, if `-frontend` empty |
| `-api-only` | No static UI; JSON API with CORS for local tooling |

---

## API contract

The SPA expects JSON shapes compatible with Python Fleuve’s admin API (non-`null` arrays for lists, objects for maps). The Go backend in `pkg/uibackend` is maintained for that compatibility. Route list: [http-api.md](./http-api.md).
