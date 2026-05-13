# Observability for bioindex

The bioindex service writes one JSON log line per HTTP request to stdout.
The container's log driver ships these to CloudWatch Logs
(`/bioindex/<env>`). That's the primary observability surface — there's
no separate metrics pipeline.

## Fields per request

`time`, `level`, `logger`, `portal`, `request_id`, `method`, `route`,
`path`, `query`, `status`, `response_bytes`, `latency_ms`, `worker_pid`.

`route` is the FastAPI route template (e.g. `/api/bio/query/{index}`),
not the literal path — so `count by route` answers "which endpoints are
getting hit" without one bucket per gene symbol.

## CloudWatch Logs Insights queries

### Requests per portal (last hour)
```
fields @timestamp, portal
| stats count() as requests by portal
| sort requests desc
```

### Top endpoints per portal
```
filter portal = "cfde"
| stats count() as requests by route
| sort requests desc
```

### p50 / p95 / p99 latency per portal per endpoint
```
stats pct(latency_ms, 50) as p50,
      pct(latency_ms, 95) as p95,
      pct(latency_ms, 99) as p99,
      count() as n
  by portal, route
| sort p95 desc
```

### Slow requests right now
```
filter latency_ms > 1000
| sort @timestamp desc
| limit 50
| display @timestamp, portal, route, path, query, latency_ms, status
```

### Errors by portal
```
filter status >= 500
| stats count() by portal, route, status
| sort count desc
```

### Bandwidth per portal
```
stats sum(response_bytes) as total_bytes,
      count() as requests
  by portal
| sort total_bytes desc
```

### Track a specific request
```
filter request_id = "<id from response header or upstream>"
```

Pass `X-Request-Id` on inbound requests to correlate with upstream logs;
the middleware honors it. Otherwise a UUID is generated per request.
