# Development

Notes for contributors working on **fleuve-go** itself (not only consuming the module).

---

## Tests

```bash
go test ./...
```

Integration-style tests may live under [`integration_test/`](../integration_test/) or use build tags; see [`.github/workflows`](../.github/workflows/) and [`docker-compose.test.yml`](../docker-compose.test.yml).

---

## Linting & formatting

Use the standard toolchain:

```bash
go fmt ./...
go vet ./...
```

---

## Regenerating the embedded UI

See [ui-embed.md](./ui-embed.md) and [`scripts/vendor-fleuve-ui.sh`](../scripts/vendor-fleuve-ui.sh). After vendoring, rebuild `fleuve-ui` so `go:embed` picks up new hashes.

---

## Module path

Imports use **`github.com/doomervibe/fleuve-go`**. Forks should use a `replace` directive in their `go.mod` or publish under a consistent module path.

---

## Documentation

- Entry: [README.md](../README.md) and [docs/README.md](./README.md).  
- When changing HTTP routes, update [http-api.md](./http-api.md) and [`pkg/gateway`](../pkg/gateway/gateway.go) / [`pkg/uibackend`](../pkg/uibackend/api.go) together.  
- Behavioral guarantees belong in [behavior-and-python-parity.md](./behavior-and-python-parity.md), not duplicated ad hoc.
