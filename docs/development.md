# Development

Notes for contributors working on **fleuve-go** itself (not only consuming the module).

---

## Tests

```bash
go test ./...
```

Integration-style tests may live under [`integration_test/`](../integration_test/) or use build tags; see [`.github/workflows`](../.github/workflows/) and [`docker-compose.test.yml`](../docker-compose.test.yml).

NATS command RPC (embedded server, `nats-server` dependency):

```bash
go test -tags=integration ./integration_test/ -run TestNATSCommandRPCGatewayToRunner -v
```

See [nats-gateway-commands.md](./nats-gateway-commands.md).

---

## Linting & formatting

Use the standard toolchain:

```bash
go fmt ./...
go vet ./...
```

---

## Regenerating the embedded UI

See [ui-embed.md](./ui-embed.md) and [`scripts/vendor-fleuve-ui.sh`](../scripts/vendor-fleuve-ui.sh). After vendoring, rebuild `./examples/ui_server` (or the `fleuve-ui` image) so `go:embed` picks up new hashes.

---

## Module path

Imports use **`github.com/doomervibe/fleuve-go`**. Forks should use a `replace` directive in their `go.mod` or publish under a consistent module path.

---

## Documentation

- Entry: [README.md](../README.md) and [docs/README.md](./README.md).  
- When changing HTTP routes, update [http-api.md](./http-api.md) and [`pkg/gateway`](../pkg/gateway/gateway.go) / [`pkg/uibackend`](../pkg/uibackend/api.go) together.  
- Behavioral guarantees belong in [behavior-and-python-parity.md](./behavior-and-python-parity.md), not duplicated ad hoc.

## Agent skills (Cursor / compatible tools)

Project skill for workflow design and wiring: [`.cursor/skills/fleuve-go/SKILL.md`](../.cursor/skills/fleuve-go/SKILL.md) (see [`reference.md`](../.cursor/skills/fleuve-go/reference.md)). Cursor loads skills from `.cursor/skills/<name>/SKILL.md`. To use the same content globally, copy or symlink the folder under `~/.cursor/skills/fleuve-go/`.
