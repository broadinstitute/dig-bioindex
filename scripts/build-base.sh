#!/usr/bin/env bash
# build-base.sh — build the bioindex-base image from dig-bioindex source
# and push to ECR. Run when dig-bioindex (app code) changes.
#
# Usage:
#   build-base.sh [--tag <tag> | --sha <sha> | --branch <branch> | --local <path>]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/audit.sh
source "$SCRIPT_DIR/lib/audit.sh"
# shellcheck source=lib/ecr.sh
source "$SCRIPT_DIR/lib/ecr.sh"

# Preflight: required external tools
for tool in aws docker git; do
    command -v "$tool" >/dev/null 2>&1 || {
        echo "ERROR: required tool '$tool' is not installed or not on PATH" >&2
        exit 1
    }
done

usage() {
    cat <<'USAGE' >&2
Usage: build-base.sh [--tag <tag> | --sha <sha> | --branch <branch> | --local <path>]
  --tag <tag>       fresh clone at git tag
  --sha <sha>       fresh clone at git commit
  --branch <branch> fresh clone at branch HEAD (default: master)
  --local <path>    use existing local clone; requires clean tree + pushed HEAD
USAGE
}

REF_FORM="--local"
REF_VALUE="$(cd "$SCRIPT_DIR/.." && pwd)"  # default: parent of scripts/

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tag|--sha|--branch|--local)
            REF_FORM="$1"
            REF_VALUE="${2:-}"
            if [[ -z "$REF_VALUE" ]]; then
                echo "ERROR: $REF_FORM requires a value" >&2; exit 1
            fi
            shift 2
            ;;
        -h|--help)
            usage; exit 0
            ;;
        *)
            echo "ERROR: unknown argument: $1" >&2; usage; exit 1
            ;;
    esac
done

# Resolve working tree (clone or local)
REPO=""
cleanup() {
    if [[ "$REF_FORM" != "--local" ]] && [[ -n "$REPO" ]] && [[ -d "$REPO" ]]; then
        rm -rf "$REPO"
    fi
}
trap cleanup EXIT

if [[ "$REF_FORM" == "--local" ]]; then
    if ! verify_clean_and_pushed "$REF_VALUE"; then
        exit 1
    fi
    REPO="$REF_VALUE"
else
    REPO=$(mktemp -d -t bioindex-base.XXXXXX)
    echo "Cloning dig-bioindex into $REPO..." >&2
    git clone --quiet --depth 50 \
        git@github.com:broadinstitute/dig-bioindex.git "$REPO"
    case "$REF_FORM" in
        --tag)
            git -C "$REPO" fetch --quiet --depth 1 origin tag "$REF_VALUE" 2>/dev/null || \
                git -C "$REPO" fetch --quiet --tags
            git -C "$REPO" checkout --quiet "$REF_VALUE"
            ;;
        --sha)
            git -C "$REPO" fetch --quiet --depth 1 origin "$REF_VALUE" 2>/dev/null || true
            git -C "$REPO" checkout --quiet "$REF_VALUE"
            ;;
        --branch)
            git -C "$REPO" checkout --quiet "$REF_VALUE"
            ;;
    esac
fi

SHA=$(git -C "$REPO" rev-parse --short=7 HEAD)
echo "Building bioindex-base:$SHA from $REPO"

if image_exists_in_ecr "bioindex-base" "$SHA"; then
    echo "Image bioindex-base:$SHA already exists in ECR; nothing to do."
    exit 0
fi

REGISTRY=$(ecr_registry_url)
BASE_URI="$REGISTRY/bioindex-base:$SHA"
BASE_LATEST="$REGISTRY/bioindex-base:latest"

echo "Building (this takes ~5 minutes — htslib compile)..."
docker build -t "$BASE_URI" -t "$BASE_LATEST" "$REPO"

echo "Logging in to ECR..."
ecr_login

echo "Pushing $BASE_URI ..."
docker push "$BASE_URI"
echo "Pushing $BASE_LATEST ..."
docker push "$BASE_LATEST"

echo
echo "=== Base image pushed ==="
echo "  bioindex-base:$SHA"
echo
echo "Next steps:"
echo "  1. In dig-bioindex-configs/BASE_IMAGE_SHA, update to: $SHA"
echo "  2. Commit + push the configs change."
echo "  3. ./scripts/deploy.sh qa --branch main"
echo
