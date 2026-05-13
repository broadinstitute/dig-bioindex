#!/usr/bin/env bash
# deploy.sh — operator-driven deploy of bioindex to QA or Prod.
#
# Usage:
#   deploy.sh <env> [--tag <tag> | --sha <sha> | --branch <branch> | --local <path>]
#                   [--no-build] [--dry-run] [--wait-timeout <seconds>]
#                   [--help-after-parse]
#
# See docs/plan-b-runbook.md for the full operator runbook.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/audit.sh
source "$SCRIPT_DIR/lib/audit.sh"
# shellcheck source=lib/ecr.sh
source "$SCRIPT_DIR/lib/ecr.sh"

usage() {
    cat <<'USAGE' >&2
Usage: deploy.sh <env> [--tag <tag> | --sha <sha> | --branch <branch> | --local <path>]
                       [--no-build] [--dry-run] [--wait-timeout <seconds>]
  <env>             qa or prod
  --tag <tag>       fresh clone at git tag
  --sha <sha>       fresh clone at git commit
  --branch <branch> fresh clone at branch HEAD (default: --branch master)
  --local <path>    use existing local clone; requires clean tree + pushed HEAD
  --no-build        refuse to build; require existing image in ECR (rollback paths)
  --dry-run         build and push but do not register task def / update service
  --wait-timeout N  seconds to wait for services-stable (default 600)
USAGE
}

ENV=""
REF_FORM=""
REF_VALUE=""
NO_BUILD=0
DRY_RUN=0
WAIT_TIMEOUT=600
HELP_AFTER_PARSE=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        qa|prod)
            ENV="$1"
            shift
            ;;
        --tag|--sha|--branch|--local)
            if [[ -n "$REF_FORM" ]]; then
                echo "ERROR: specify exactly one of --tag/--sha/--branch/--local" >&2
                usage; exit 1
            fi
            REF_FORM="$1"
            REF_VALUE="${2:-}"
            if [[ -z "$REF_VALUE" ]]; then
                echo "ERROR: $REF_FORM requires a value" >&2
                exit 1
            fi
            shift 2
            ;;
        --no-build)
            NO_BUILD=1; shift
            ;;
        --dry-run)
            DRY_RUN=1; shift
            ;;
        --wait-timeout)
            WAIT_TIMEOUT="${2:-600}"; shift 2
            ;;
        --help-after-parse)
            HELP_AFTER_PARSE=1; shift
            ;;
        -h|--help)
            usage; exit 0
            ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            usage; exit 1
            ;;
    esac
done

if [[ -z "$ENV" ]]; then
    echo "ERROR: env argument is required (qa or prod)" >&2
    usage; exit 1
fi

if [[ -z "$REF_FORM" ]]; then
    REF_FORM="--branch"
    REF_VALUE="master"
fi

if [[ "$REF_FORM" == "--local" ]] && [[ ! -d "$REF_VALUE" ]]; then
    echo "ERROR: --local path does not exist: $REF_VALUE" >&2
    exit 1
fi

if [[ "$HELP_AFTER_PARSE" -eq 1 ]]; then
    echo "Args parsed OK: env=$ENV ref_form=$REF_FORM ref_value=$REF_VALUE"
    exit 0
fi

# Subsequent steps (resolve tree, build, push, deploy) are added in Tasks 1.6+.
echo "deploy.sh arg parsing OK; remainder of pipeline not yet implemented." >&2
exit 0
