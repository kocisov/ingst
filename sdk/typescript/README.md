# @ingst/sdk

> TypeScript client for the ingst event ingestion API. Use it in trusted
> server-side applications; do not expose an ingst API key in browser code.

Run the complete example with:

```sh
INGST_API_KEY='ingst_...' bun example/index.ts
```

Set `INGST_URL` to override the default `http://localhost:3000`. The example
creates an idempotent, typed event under a ten-second timeout, handles
`IngstApiError`, and iterates through filtered event pages.

`IngstApiError` exposes the HTTP `status`, parsed response `body`, and
`requestId` returned by the server.
