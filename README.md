# ingst

> Go and PostgreSQL event ingestion server

## Run

```sh
docker compose up -d
export DATABASE_URL='postgres://ingst:ingst@localhost:5432/ingst?sslmode=disable'
go run . migrate
go run . create-api-key local
go run .
```

Run the tests with `go test ./...`.

The key creation command prints a generated API key once. Store it in your
secret manager. The server listens on `PORT` (default `3000`) and requires
`DATABASE_URL`. `MAX_BODY_BYTES` defaults to 1 MiB. Migrations are explicit so
runtime database credentials do not need DDL access.

Revoke a key using the prefix returned when it was created:

```sh
go run . revoke-api-key ingst_abcd1234
```

Run the PostgreSQL integration test against a dedicated test database with:

```sh
TEST_DATABASE_URL='postgres://ingst:ingst@localhost:5432/ingst?sslmode=disable' go test ./...
```

## API

Ingest an event:

```sh
curl -X POST http://localhost:3000/events \
  -H 'authorization: Bearer ingst_your-generated-key' \
  -H 'content-type: application/json' \
  -H 'idempotency-key: order-42-created' \
  -d '{
    "source": "checkout",
    "type": "order.created",
    "timestamp": "2026-08-14T10:00:00Z",
    "payload": {"orderId": 42}
  }'
```

Read events, newest first:

```sh
curl -H 'authorization: Bearer ingst_your-generated-key' \
  'http://localhost:3000/events?source=checkout&type=order.created&limit=100'
```

Include `Authorization: Bearer <key>` on both event endpoints. API keys are
stored as SHA-256 hashes and can be revoked without restarting the service.
`source` and `type` filters are optional. `limit` defaults to 100 and is capped
at 1000. When another page exists, the response includes `next_cursor`; pass
that value as the next request's `cursor` query parameter.

`GET /health` is a process liveness check. `GET /ready` checks PostgreSQL and is
intended for load balancers and orchestrators. Authentication and distributed
rate limiting should also be enforced at the deployment gateway.
