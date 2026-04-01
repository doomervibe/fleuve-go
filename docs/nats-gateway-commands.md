# Gateway commands via NATS (request/reply)

When **`gateway_commands_via_nats`** is enabled on **both** `fleuve-gateway` and `fleuve-runner`, HTTP command and lifecycle routes do not call PostgreSQL from the gateway process for those operations. The gateway sends **core NATS** request/reply messages; the runner executes `CreateNew`, `ProcessCommand`, `PauseWorkflow`, `ResumeWorkflow`, and `CancelWorkflow` on its **`PGXRepo`** and updates the in-memory **subscription cache** the same way as stream-driven processing.

## Configuration

| Mechanism | Value |
|-----------|--------|
| TOML (`[fleuve]`) | `gateway_commands_via_nats = true` |
| Environment | `FLEUVE_GATEWAY_COMMANDS_VIA_NATS=true` |
| Prerequisite | `nats_url` / `FLEUVE_NATS_URL` must be set |

- **`enable_jetstream`**: optional for this RPC path (core NATS only). If you also use JetStream for the event stream, keep `enable_jetstream = true` and run the runner with a JetStream reader; the runner then uses one NATS connection for JetStream publish (when configured) and the same connection for command RPC when both flags are on.
- **Order of startup**: start **runner** before the gateway (or ensure queue subscribers are up), otherwise HTTP clients see errors until a responder is available.

## Subjects and queue group

All subjects are prefixed with `fleuve.command.rpc.<WorkflowTypeName>.`:

| Operation | Subject suffix |
|-----------|----------------|
| Create workflow | `.create` |
| Process command | `.process` |
| Pause | `.pause` |
| Resume | `.resume` |
| Cancel | `.cancel` |

Consumers use queue group **`fleuve-command-rpc`** so only one runner instance handles each request when you scale horizontally.

## Cancel and ActionExecutor

Cancel RPC on the runner uses the **`ActionExecutor` instance started in the runner process**, not the gateway’s pointer. The gateway and runner must share the **same database** and compatible activity configuration so cancel semantics match a single-process deployment.

## Trace context (HTTP → NATS)

The gateway attaches W3C-style headers to the NATS request when present on the HTTP request: **`Traceparent`**, **`Tracestate`**, **`Baggage`**. Downstream observability can read them from the NATS message headers on the runner side if you add instrumentation.

## Integration test

Embedded NATS server (adds the `nats-server` module dependency):

```bash
go test -tags=integration ./integration_test/ -run TestNATSCommandRPCGatewayToRunner -v
```

## Python parity

This path is specific to **fleuve-go** deployment topology. Python Fleuve does not define the same RPC surface; keep **one active runner stack** per stream scope (see [behavior-and-python-parity.md](./behavior-and-python-parity.md)).
