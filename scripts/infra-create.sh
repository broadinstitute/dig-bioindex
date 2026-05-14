#!/usr/bin/env bash
# infra-create.sh — create the CloudFormation stack for an env.
# Run once per env. Subsequent infra changes use infra-update.sh.
#
# Usage: infra-create.sh <env>
set -euo pipefail

# Preflight
command -v aws >/dev/null 2>&1 || { echo "ERROR: aws CLI is required" >&2; exit 1; }

if [[ $# -ne 1 ]]; then
    echo "Usage: infra-create.sh <env>  (qa or prod)" >&2
    exit 1
fi
ENV="$1"
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

aws cloudformation create-stack \
    --stack-name "$STACK_NAME" \
    --template-body "file://$TEMPLATE" \
    --parameters "file://$PARAMS_FILE" \
    --capabilities CAPABILITY_NAMED_IAM \
    >/dev/null

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
