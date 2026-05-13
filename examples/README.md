# Running bioindex locally with example configs

This directory shows the configs-dir shape the consolidated bioindex
service expects at runtime. Copy `portals/example.yaml`, fill in real
values from your AWS environment, and point the server at the directory.

## Run

```bash
# Point the server at this configs directory
export BIOINDEX_CONFIG_DIR=$(pwd)/configs
export BIOINDEX_ENV=qa

# Run
python -m bioindex.main serve --port 5000
```

The Dockerfile ships with a default `BIOINDEX_TOKEN_SIGNING_KEY`. Bioindex
serves only public data, so a stable in-image key is acceptable: forging a
token at most yields garbled iteration over already-public data, not an
authorization bypass. Override at runtime via `-e BIOINDEX_TOKEN_SIGNING_KEY=...`
if you want unique keys per environment.

Then visit `http://localhost:5000/example/api/bio/indexes`.

## What lives where

- `envs/qa.yaml`, `envs/prod.yaml` — env-wide defaults merged into every
  portal's config block for the matching env.
- `portals/<name>.yaml` — one portal per file. Each portal has independent
  `envs.qa` and `envs.prod` blocks; any `Config` field is per-env-overridable.

To add more portals, add more files under `portals/`. To skip a portal in
some env, omit its `envs.<env>` block.

## Smoke checks (after `serve` is running)

```bash
# liveness — should always return 200
curl -fsS http://localhost:5000/health

# readiness — runs SELECT 1 against every portal's bio engine
curl -fsS http://localhost:5000/ready | jq .

# unknown portal — should return 404 with a list of valid portals
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:5000/nope/api/bio/indexes

# list indexes for the example portal
curl -fsS http://localhost:5000/example/api/bio/indexes | jq .

# query against an index (replace <index> and <q> with real values)
curl -fsS "http://localhost:5000/example/api/bio/query/<index>?q=<q>" | jq .

# pagination smoke — capture the continuation token and replay it
TOKEN=$(curl -fsS "http://localhost:5000/example/api/bio/query/<index>?q=<q>" | jq -r .continuation)
curl -fsS "http://localhost:5000/example/api/bio/cont?token=$TOKEN" | jq .
```

A pagination round-trip exercises stateless tokens (Section 3 of the design): the token from page 1 must work even when page 2 lands on a different uvicorn worker.
