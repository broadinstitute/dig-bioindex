# Running bioindex locally with example configs

This directory shows the configs-dir shape the consolidated bioindex
service expects at runtime. Copy `portals/example.yaml`, fill in real
values from your AWS environment, and point the server at the directory.

## Run

```bash
export BIOINDEX_CONFIG_DIR=$(pwd)/configs
export BIOINDEX_ENV=qa

python -m bioindex.main serve --port 5000 --dev
```

`--dev` auto-generates an ephemeral `BIOINDEX_TOKEN_SIGNING_KEY` if none is set,
so local dev needs no key (tokens won't survive a restart). In production, set
`BIOINDEX_TOKEN_SIGNING_KEY` explicitly and drop `--dev`. Bioindex serves only
public data, so a forged token at most yields garbled iteration over
already-public data, not an authorization bypass.

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
# NOTE: /health and /ready are not yet implemented (deferred); these will 404.
# curl -fsS http://localhost:5000/health
# curl -fsS http://localhost:5000/ready | jq .

curl -s -o /dev/null -w "%{http_code}\n" http://localhost:5000/nope/api/bio/indexes

curl -fsS http://localhost:5000/example/api/bio/indexes | jq .

curl -fsS "http://localhost:5000/example/api/bio/query/<index>?q=<q>" | jq .

TOKEN=$(curl -fsS "http://localhost:5000/example/api/bio/query/<index>?q=<q>" | jq -r .continuation)
curl -fsS "http://localhost:5000/example/api/bio/cont?token=$TOKEN" | jq .
```
