#!/usr/bin/env bash
# infra-update.sh — update an existing CloudFormation stack.
# Use when bioindex-stack.yaml or parameters/<env>.json changes.
#
# Usage: infra-update.sh <env>
set -euo pipefail

command -v aws >/dev/null 2>&1 || { echo "ERROR: aws CLI is required" >&2; exit 1; }

if [[ $# -ne 1 ]]; then
    echo "Usage: infra-update.sh <env>  (qa or prod)" >&2; exit 1
fi
ENV="$1"
if [[ "$ENV" != "qa" && "$ENV" != "prod" ]]; then
    echo "ERROR: env must be qa or prod (got: $ENV)" >&2; exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE="$SCRIPT_DIR/../infra/bioindex-stack.yaml"
PARAMS_FILE="$SCRIPT_DIR/../infra/parameters/${ENV}.json"

[ -f "$TEMPLATE" ] || { echo "ERROR: template not found: $TEMPLATE" >&2; exit 1; }
[ -f "$PARAMS_FILE" ] || { echo "ERROR: params not found: $PARAMS_FILE" >&2; exit 1; }

STACK_NAME="bioindex-${ENV}"
echo "Updating CloudFormation stack: $STACK_NAME"

aws cloudformation update-stack \
    --stack-name "$STACK_NAME" \
    --template-body "file://$TEMPLATE" \
    --parameters "file://$PARAMS_FILE" \
    --capabilities CAPABILITY_NAMED_IAM \
    >/dev/null

echo "Waiting for stack update..."
aws cloudformation wait stack-update-complete \
    --stack-name "$STACK_NAME"
echo "Stack $STACK_NAME updated."
