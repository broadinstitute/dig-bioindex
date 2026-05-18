#!/usr/bin/env bash
# infra-create.sh — create the CloudFormation stack for an env.
# Run once per env. Subsequent infra changes use infra-update.sh.
#
# Usage: infra-create.sh <env> [--no-rollback]
#   --no-rollback   on failure, leave resources in place for inspection
#                   (use when debugging service / task start failures).
set -euo pipefail

# Preflight
command -v aws >/dev/null 2>&1 || { echo "ERROR: aws CLI is required" >&2; exit 1; }

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "Usage: infra-create.sh <env> [--no-rollback]  (env: qa or prod)" >&2
    exit 1
fi
ENV="$1"
NO_ROLLBACK=0
if [[ $# -eq 2 ]]; then
    if [[ "$2" == "--no-rollback" ]]; then
        NO_ROLLBACK=1
    else
        echo "ERROR: unknown flag: $2 (expected --no-rollback)" >&2
        exit 1
    fi
fi
if [[ "$ENV" != "qa" && "$ENV" != "prod" ]]; then
    echo "ERROR: env must be qa or prod (got: $ENV)" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE="$SCRIPT_DIR/../infra/bioindex-stack.yaml"
PARAMS_FILE="$SCRIPT_DIR/../infra/parameters/${ENV}.json"

[ -f "$TEMPLATE" ] || { echo "ERROR: template not found: $TEMPLATE" >&2; exit 1; }
[ -f "$PARAMS_FILE" ] || { echo "ERROR: params not found: $PARAMS_FILE" >&2; exit 1; }

STACK_NAME="bioindex-${ENV}"
echo "Creating CloudFormation stack: $STACK_NAME"

CREATE_ARGS=(
    --stack-name "$STACK_NAME"
    --template-body "file://$TEMPLATE"
    --parameters "file://$PARAMS_FILE"
    --capabilities CAPABILITY_NAMED_IAM
)
if [[ "$NO_ROLLBACK" -eq 1 ]]; then
    echo "(--no-rollback set: failed resources will be left in place for inspection)"
    CREATE_ARGS+=(--on-failure DO_NOTHING)
fi

aws cloudformation create-stack "${CREATE_ARGS[@]}" >/dev/null

echo "Waiting for stack creation (~5 min)..."
aws cloudformation wait stack-create-complete \
    --stack-name "$STACK_NAME"
echo "Stack $STACK_NAME created."

echo
echo "=== Stack Outputs ==="
aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --query 'Stacks[0].Outputs' \
    --output table
