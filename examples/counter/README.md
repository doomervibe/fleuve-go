# Counter example

Direct use of [`pkg/repo`](../../pkg/repo) with [`pkg/counterworkflow`](../../pkg/counterworkflow): creates workflow `counter-001`, runs increments, reset, and concurrent updates. Writes to PostgreSQL only (no gateway or runner required).

## Run

From the repository root:

```bash
export FLEUVE_DATABASE_URL="postgresql://user:pass@localhost:5432/fleuve?sslmode=disable"
go run ./examples/counter
```

The program waits for Ctrl+C after finishing the scripted steps.

## Notes

- Uses a fixed id `counter-001`. If it already exists, `CreateNew` fails; delete rows from `stored_events` for that id or change the id in `main.go`.
- Use the **same** `FLEUVE_DATABASE_URL` as `fleuve-ui` if you want the admin UI to list this workflow.

More examples: [../README.md](../README.md).
