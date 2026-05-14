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

# Preflight: required external tools.
for tool in aws docker git jq; do
    command -v "$tool" >/dev/null 2>&1 || {
        echo "ERROR: required tool '$tool' is not installed or not on PATH" >&2
        exit 1
    }
done

# On macOS, GNU coreutils provides 'timeout' as 'gtimeout'. Allow either.
if command -v timeout >/dev/null 2>&1; then
    TIMEOUT_CMD="timeout"
elif command -v gtimeout >/dev/null 2>&1; then
    TIMEOUT_CMD="gtimeout"
else
    echo "ERROR: 'timeout' (GNU coreutils) is required (on macOS: brew install coreutils)" >&2
    exit 1
fi

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

resolve_configs_tree() {
    local form="$1"
    local value="$2"
    local out_path_var="$3"
    local out_sha_var="$4"

    if [[ "$form" == "--local" ]]; then
        if ! verify_clean_and_pushed "$value"; then
            return 1
        fi
        local sha
        sha=$(git -C "$value" rev-parse --short=7 HEAD)
        printf -v "$out_path_var" '%s' "$value"
        printf -v "$out_sha_var" '%s' "$sha"
        return 0
    fi

    # Clone fresh. Record the temp path FIRST so the EXIT trap can clean
    # it up even if a subsequent git command fails.
    local clone_dir
    clone_dir=$(mktemp -d -t bioindex-configs.XXXXXX)
    printf -v "$out_path_var" '%s' "$clone_dir"

    echo "Cloning dig-bioindex-configs into $clone_dir..." >&2
    git clone --quiet --depth 50 \
        git@github.com:broadinstitute/dig-bioindex-configs.git "$clone_dir"

    case "$form" in
        --tag)
            git -C "$clone_dir" fetch --quiet --depth 1 origin tag "$value" 2>/dev/null || \
                git -C "$clone_dir" fetch --quiet --tags
            git -C "$clone_dir" checkout --quiet "$value"
            ;;
        --sha)
            git -C "$clone_dir" fetch --quiet --depth 1 origin "$value" 2>/dev/null || true
            git -C "$clone_dir" checkout --quiet "$value"
            ;;
        --branch)
            git -C "$clone_dir" checkout --quiet "$value"
            ;;
        *)
            echo "ERROR: unexpected ref form $form" >&2
            return 1
            ;;
    esac

    local sha
    sha=$(git -C "$clone_dir" rev-parse --short=7 HEAD)
    printf -v "$out_sha_var" '%s' "$sha"
}

# Initialize + register cleanup BEFORE resolve_configs_tree runs, so the
# trap can clean up the temp dir if a git operation inside fails.
CONFIGS_PATH=""
CONFIGS_SHA=""
cleanup() {
    if [[ -n "${KEEP_CLONE:-}" ]]; then
        echo "KEEP_CLONE set; not removing $CONFIGS_PATH" >&2
        return 0
    fi
    if [[ "$REF_FORM" != "--local" ]] && [[ -n "$CONFIGS_PATH" ]] && [[ -d "$CONFIGS_PATH" ]]; then
        rm -rf "$CONFIGS_PATH"
    fi
}
trap cleanup EXIT
# (The clone is removed on both success and failure paths. To inhibit
# cleanup for debugging a failed deploy, set KEEP_CLONE=1 in the env.)

resolve_configs_tree "$REF_FORM" "$REF_VALUE" CONFIGS_PATH CONFIGS_SHA
echo "Configs tree: $CONFIGS_PATH @ $CONFIGS_SHA"

# Read BASE_IMAGE_SHA from the configs tree
if [[ ! -f "$CONFIGS_PATH/BASE_IMAGE_SHA" ]]; then
    echo "ERROR: BASE_IMAGE_SHA file not found in configs tree" >&2
    exit 1
fi
BASE_IMAGE_SHA=$(tr -d '[:space:]' < "$CONFIGS_PATH/BASE_IMAGE_SHA")
if [[ -z "$BASE_IMAGE_SHA" ]]; then
    echo "ERROR: BASE_IMAGE_SHA file is empty" >&2
    exit 1
fi

if ! image_exists_in_ecr "bioindex-base" "$BASE_IMAGE_SHA"; then
    echo "ERROR: bioindex-base:$BASE_IMAGE_SHA does not exist in ECR." >&2
    echo "       Run: ./scripts/build-base.sh --sha $BASE_IMAGE_SHA" >&2
    exit 1
fi
echo "Base image: bioindex-base:$BASE_IMAGE_SHA (exists)"

# Check if deployable image already exists
DEPLOYABLE_TAG="$CONFIGS_SHA"
SKIP_BUILD=0
if image_exists_in_ecr "bioindex-deployable" "$DEPLOYABLE_TAG"; then
    if [[ "$NO_BUILD" -eq 1 ]]; then
        echo "Deployable image already in ECR; --no-build set, skipping build."
    else
        echo "Deployable image bioindex-deployable:$DEPLOYABLE_TAG already in ECR; skipping build."
    fi
    SKIP_BUILD=1
else
    if [[ "$NO_BUILD" -eq 1 ]]; then
        echo "ERROR: --no-build specified but bioindex-deployable:$DEPLOYABLE_TAG not in ECR." >&2
        exit 1
    fi
fi

REGISTRY=$(ecr_registry_url)
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
AWS_REGION="${AWS_REGION:-us-east-1}"
DEPLOYABLE_URI="$REGISTRY/bioindex-deployable:$DEPLOYABLE_TAG"
DEPLOYABLE_LATEST="$REGISTRY/bioindex-deployable:${ENV}-latest"

if [[ "$SKIP_BUILD" -eq 0 ]]; then
    echo "Building image..."
    docker build \
        --build-arg AWS_ACCOUNT_ID="$AWS_ACCOUNT_ID" \
        --build-arg AWS_REGION="$AWS_REGION" \
        --build-arg BASE_IMAGE_SHA="$BASE_IMAGE_SHA" \
        -t "$DEPLOYABLE_URI" \
        -t "$DEPLOYABLE_LATEST" \
        "$CONFIGS_PATH"

    echo "Logging in to ECR..."
    ecr_login

    echo "Pushing $DEPLOYABLE_URI ..."
    docker push "$DEPLOYABLE_URI"
    echo "Pushing $DEPLOYABLE_LATEST ..."
    docker push "$DEPLOYABLE_LATEST"
else
    # Still re-tag <env>-latest to point at the existing image
    echo "Logging in to ECR for re-tag of <env>-latest..."
    ecr_login
    docker pull "$DEPLOYABLE_URI" >/dev/null
    docker tag "$DEPLOYABLE_URI" "$DEPLOYABLE_LATEST"
    docker push "$DEPLOYABLE_LATEST"
fi

if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "--dry-run: image is in ECR but service NOT updated. Exiting."
    exit 0
fi

CLUSTER="bioindex-${ENV}"
SERVICE="bioindex-${ENV}"
TASK_FAMILY="bioindex-${ENV}"

# Capture the previous deployed image (for the rollback hint at the end)
PREV_TASK_DEF_ARN=$(aws ecs describe-services \
    --cluster "$CLUSTER" --services "$SERVICE" \
    --query 'services[0].taskDefinition' --output text)
PREV_IMAGE=$(aws ecs describe-task-definition \
    --task-definition "$PREV_TASK_DEF_ARN" \
    --query 'taskDefinition.containerDefinitions[0].image' --output text 2>/dev/null \
    || echo "(no previous deployment)")
PREV_TAG="${PREV_IMAGE##*:}"

# Render new task definition by patching the current one
echo "Registering new task definition revision..."
CURRENT_DEF=$(aws ecs describe-task-definition \
    --task-definition "$TASK_FAMILY" \
    --query 'taskDefinition' --output json)

NEW_DEF=$(echo "$CURRENT_DEF" | jq \
    --arg new_image "$DEPLOYABLE_URI" \
    '
    .containerDefinitions[0].image = $new_image
    | del(.taskDefinitionArn, .revision, .status, .requiresAttributes,
          .compatibilities, .registeredAt, .registeredBy)
    ')

NEW_TASK_DEF_ARN=$(aws ecs register-task-definition \
    --cli-input-json "$NEW_DEF" \
    --query 'taskDefinition.taskDefinitionArn' --output text)
echo "Registered: $NEW_TASK_DEF_ARN"

echo "Updating service to new task definition..."
aws ecs update-service \
    --cluster "$CLUSTER" --service "$SERVICE" \
    --task-definition "$NEW_TASK_DEF_ARN" \
    >/dev/null

echo "Waiting for services-stable (timeout ${WAIT_TIMEOUT}s)..."
SECONDS=0
START_TS=$SECONDS
if ! "$TIMEOUT_CMD" "$WAIT_TIMEOUT" aws ecs wait services-stable \
        --cluster "$CLUSTER" --services "$SERVICE"; then
    echo "WARNING: services-stable did not succeed within ${WAIT_TIMEOUT}s." >&2
    echo "         Check CloudWatch Logs and consider a rollback." >&2
    exit 1
fi
ELAPSED=$((SECONDS - START_TS))

echo
echo "=== Deploy summary ==="
echo "  Env:                 $ENV"
echo "  Configs SHA:         $CONFIGS_SHA"
echo "  Base image SHA:      $BASE_IMAGE_SHA"
echo "  Image deployed:      $DEPLOYABLE_URI"
echo "  Task definition:     $NEW_TASK_DEF_ARN"
echo "  Wait elapsed:        ${ELAPSED}s"
echo
if [[ "$PREV_TAG" =~ ^[0-9a-f]{7,40}$ ]]; then
    echo "  Rollback if needed:  ./scripts/deploy.sh $ENV --sha $PREV_TAG"
else
    echo "  (no previous deployment to roll back to)"
fi
echo
exit 0
