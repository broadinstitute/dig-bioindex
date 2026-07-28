# Example config directory

The shape of the config directory `serve` reads when it is asked to host more
than one portal. Copy `portals/example.yaml`, fill in real values, and point
`BIOINDEX_CONFIG_DIR` at the directory.

To run a single portal locally you don't need any of this - leave
`BIOINDEX_CONFIG_DIR` unset and `serve` builds one portal from your
`.bioindex` environment file. See "Starting the Server" in the top-level
[README](../README.md).

## Run

```bash
export BIOINDEX_CONFIG_DIR=$(pwd)/configs
export BIOINDEX_ENV=qa

bioindex serve --port 5000 --dev
```

Then visit `http://localhost:5000/example/api/bio/indexes`.

## What lives where

- `envs/<env>.yaml` — defaults merged into every portal's block for that env.
- `portals/<name>.yaml` — one portal per file, with an `envs.<env>` block per
  environment it runs in. Anything settable via `BIOINDEX_*` can go in either,
  and the portal block wins.

Add a portal by adding a file under `portals/`. Skip a portal in some
environment by omitting its `envs.<env>` block.

## Smoke checks

```bash
# unknown portal -> 404 listing the valid ones
curl -s http://localhost:5000/nope/api/bio/indexes | jq .

curl -fsS http://localhost:5000/example/api/bio/indexes | jq .

# paginate: the continuation token is bound to the portal that issued it
TOKEN=$(curl -fsS "http://localhost:5000/example/api/bio/query/<index>?q=<q>" | jq -r .continuation)
curl -fsS "http://localhost:5000/example/api/bio/cont?token=$TOKEN" | jq .
```
