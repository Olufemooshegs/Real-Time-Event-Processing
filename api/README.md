# Analytics API

This is the Step 10 read-only API over the existing `transactions` schema. It uses raw
parameterized asyncpg queries, a lifespan-managed pool, and keyset pagination. It does
not create tables or expose write endpoints.

Run with Compose after copying `.env.example` to `.env`:

```bash
make up
curl http://localhost:8000/health
```

The pool defaults to 2-10 connections for the single development Postgres instance. Pool
acquisition fails fast after two seconds with HTTP 503 instead of queueing indefinitely.
