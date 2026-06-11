#!/usr/bin/env bash
# local_e2e.sh — local full-stack end-to-end test for the cache + rebuild-invalidation feature.
#
# Prerequisites:
#   - Docker (with Docker Compose v2) installed and the daemon running.
#   - AWS credentials with read access to the dig-bio-index S3 bucket (used to
#     stream actual record bytes during index build and query).
#   - AWS credentials that can also resolve the BIOINDEX_RDS_SECRET secret via
#     Secrets Manager (the app reads DB creds from there), OR a portal YAML that
#     points directly at the local Docker MySQL (see NOTE below).
#   - Python env with bioindex + dependencies installed (e.g. `pip install -e .`).
#
# Usage:
#   ./scripts/local_e2e.sh <index-name> <s3-prefix/> <schema>
#
#   <index-name>   Arbitrary name for the local test index (e.g. "e2e-smoke").
#   <s3-prefix/>   A SMALL prefix under dig-bio-index that already has indexed
#                  JSON files, trailing slash required (e.g. "gene/part1/").
#   <schema>       Index schema string matching the data (e.g. "gene").
#
# NOTE on MySQL connectivity:
#   The app resolves its DB credentials via BIOINDEX_RDS_SECRET (AWS Secrets
#   Manager) or BIOINDEX_RDS_INSTANCE (AWS RDS describe).  To use the local
#   Docker MySQL started by this script you must either:
#     (a) Have an AWS secret (e.g. "dig-bio-index-local") that contains:
#           {"engine":"mysql","host":"127.0.0.1","port":3306,
#            "username":"dataregistry","password":"dataregistry",
#            "dbInstanceIdentifier":"local"}
#         and set BIOINDEX_RDS_SECRET to that secret name in .bioindex or the
#         portal YAML, OR
#     (b) Accept that the CLI and serve commands connect to your normally
#         configured RDS target (remote/real) and the Docker MySQL is unused.
#   Option (b) is fine for smoke-testing the cache/continuation feature; the
#   Docker MySQL is provided for operators who want a clean, isolated schema.
#
# This script is a MANUAL operator gate — it is NOT run by pytest.
#
set -euo pipefail
cd "$(dirname "$0")/.."

# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------
INDEX="${1:-e2e-smoke}"
PREFIX="${2:?usage: $0 <index> <s3-prefix/> <schema>}"
SCHEMA="${3:?usage: $0 <index> <s3-prefix/> <schema>}"
TABLE="${INDEX//-/_}"
PORTAL="local"
PORT=5000
ENV_FILE=".bioindex"

# ---------------------------------------------------------------------------
# Load local env (sets BIOINDEX_S3_BUCKET, BIOINDEX_RDS_SECRET, etc.)
# ---------------------------------------------------------------------------
if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC2046
  export $(grep -v '^#' "$ENV_FILE" | xargs)
fi

# ---------------------------------------------------------------------------
# Start local MySQL (used when BIOINDEX_RDS_SECRET points to localhost)
# ---------------------------------------------------------------------------
echo "==> Starting local MySQL via docker compose..."
docker compose -f docker-compose.local.yml up -d

echo "==> Waiting for MySQL to be ready..."
until docker compose -f docker-compose.local.yml exec -T mysql \
      mysqladmin ping -h localhost -pdataregistry >/dev/null 2>&1; do
  sleep 2
done
echo "    MySQL is ready."

# ---------------------------------------------------------------------------
# Build the portal config that serve needs (BIOINDEX_CONFIG_DIR + BIOINDEX_ENV)
# ---------------------------------------------------------------------------
TMPDIR_CFG="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_CFG"' EXIT

mkdir -p "$TMPDIR_CFG/portals" "$TMPDIR_CFG/envs"

# Minimal env defaults (no per-env overrides needed for local single-portal)
cat > "$TMPDIR_CFG/envs/local.yaml" <<ENVYAML
BIOINDEX_RESPONSE_LIMIT: 1048576
BIOINDEX_RESPONSE_LIMIT_MAX: 52428800
BIOINDEX_MATCH_LIMIT: 100
ENVYAML

# Portal YAML — inherits BIOINDEX_S3_BUCKET and BIOINDEX_RDS_SECRET from the
# operator's environment (loaded from .bioindex above); override BIO_SCHEMA
# to match the Docker MySQL database "bio".
cat > "$TMPDIR_CFG/portals/${PORTAL}.yaml" <<PORTALYAML
name: ${PORTAL}
envs:
  local:
    BIOINDEX_S3_BUCKET: ${BIOINDEX_S3_BUCKET:-dig-bio-index}
    BIOINDEX_RDS_SECRET: ${BIOINDEX_RDS_SECRET:-dig-bio-index}
    BIOINDEX_BIO_SCHEMA: bio
PORTALYAML

export BIOINDEX_CONFIG_DIR="$TMPDIR_CFG"
export BIOINDEX_ENV="local"

# ---------------------------------------------------------------------------
# Migrate schema + register the test index
# (create internally runs migrate; there is no standalone migrate subcommand)
# ---------------------------------------------------------------------------
echo "==> Creating index '$INDEX' ..."
# -e flag belongs to the parent cli group, before the subcommand name
yes | python -m bioindex.main -e "$ENV_FILE" create "$INDEX" "$TABLE" "$PREFIX" "$SCHEMA"

echo "==> Building index '$INDEX' ..."
yes | python -m bioindex.main -e "$ENV_FILE" index "$INDEX"

# ---------------------------------------------------------------------------
# Start the server (serve does not use -e; it reads BIOINDEX_CONFIG_DIR/ENV)
# ---------------------------------------------------------------------------
echo "==> Starting bioindex server on port $PORT ..."
python -m bioindex.main serve -p "$PORT" &
SERVER_PID=$!

# On exit: kill server + tear down compose
trap 'echo "==> Cleaning up..."; kill "$SERVER_PID" 2>/dev/null || true; \
      docker compose -f docker-compose.local.yml down; \
      rm -rf "$TMPDIR_CFG"' EXIT

echo "==> Waiting for server to be ready..."
until curl -sf "http://localhost:${PORT}/ready" >/dev/null 2>&1; do
  sleep 1
done
echo "    Server is ready."

# ---------------------------------------------------------------------------
# Fetch a queryable key from the keys endpoint (arity=1 assumed)
# The keys endpoint returns: {"index": "...", "keys": [...], "nonce": "..."}
# ---------------------------------------------------------------------------
KEYS_URL="http://localhost:${PORT}/${PORTAL}/api/bio/keys/${INDEX}/1"
echo "==> Fetching keys from $KEYS_URL ..."
KEY="$(curl -sf "$KEYS_URL" | python -c 'import sys,json; d=json.load(sys.stdin); print(d["keys"][0])')"
echo "    Using key: $KEY"

QUERY_URL="http://localhost:${PORT}/${PORTAL}/api/bio/query/${INDEX}?q=${KEY}"

# ---------------------------------------------------------------------------
# Test 1: First request must be X-Cache: MISS, second must be X-Cache: HIT
# ---------------------------------------------------------------------------
echo "==> Test 1: cache MISS then HIT..."
c1=$(curl -s -D - "$QUERY_URL" -o /tmp/r1.json \
     | awk 'tolower($1)=="x-cache:"{print $2}' | tr -d '\r\n')
c2=$(curl -s -D - "$QUERY_URL" -o /tmp/r2.json \
     | awk 'tolower($1)=="x-cache:"{print $2}' | tr -d '\r\n')

if [ "$c1" != "MISS" ] || [ "$c2" != "HIT" ]; then
  echo "FAIL: expected MISS then HIT, got '$c1' then '$c2'"
  exit 1
fi

# Verify data identity and nonce freshness
python3 - <<'PY'
import json, sys
a = json.load(open('/tmp/r1.json'))
b = json.load(open('/tmp/r2.json'))
assert a['data'] == b['data'], f"cached data differs: {len(a['data'])} vs {len(b['data'])} records"
assert a['nonce'] != b['nonce'], "nonce should be fresh per response, got same value"
print("    OK: identical data, fresh nonce, cache HIT confirmed")
PY

# ---------------------------------------------------------------------------
# Test 2: Stale continuation → HTTP 409 after version mutation
#
# The UPDATE below simulates an index rebuild by appending 'X' to the version
# of every built key for this index.  That changes the per-index generation
# fingerprint, so the subsequent /cont with the token minted before the bump
# must be rejected with 409 (stale continuation).
# ---------------------------------------------------------------------------
TOKEN="$(python3 -c 'import json; print(json.load(open("/tmp/r1.json")).get("continuation") or "")')"

if [ -n "$TOKEN" ]; then
  echo "==> Test 2: stale continuation token → 409..."

  # Bump version on all built keys for this index so the fingerprint changes.
  # This simulates a rebuild that updates key versions.
  docker compose -f docker-compose.local.yml exec -T mysql \
    mysql -u dataregistry -pdataregistry bio \
    -e "UPDATE \`__Keys\` SET \`version\`=CONCAT(\`version\`,'X') WHERE \`index\`='${INDEX}' AND \`built\` IS NOT NULL;"

  HTTP_CODE="$(curl -s -o /dev/null -w '%{http_code}' \
    "http://localhost:${PORT}/${PORTAL}/api/bio/cont?token=${TOKEN}")"

  if [ "$HTTP_CODE" = "409" ]; then
    echo "    OK: rebuild invalidated old continuation (409)"
  else
    echo "FAIL: stale cont expected 409, got $HTTP_CODE"
    exit 1
  fi
else
  echo "==> Test 2: skipped (first response had no continuation token — all records fit in one page)"
fi

echo ""
echo "LOCAL E2E PASSED"
