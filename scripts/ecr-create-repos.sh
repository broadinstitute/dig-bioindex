#!/usr/bin/env bash
# ecr-create-repos.sh — one-time creation of the bioindex ECR repos with
# lifecycle policies. Run once per AWS account.
set -euo pipefail

command -v aws >/dev/null 2>&1 || { echo "ERROR: aws CLI is required" >&2; exit 1; }

REGION="${AWS_REGION:-us-east-1}"

for repo in bioindex-base bioindex-deployable; do
    if aws ecr describe-repositories --repository-names "$repo" --region "$REGION" >/dev/null 2>&1; then
        echo "$repo already exists."
    else
        echo "Creating $repo..."
        aws ecr create-repository --repository-name "$repo" --region "$REGION" >/dev/null
    fi
done

# Lifecycle: keep most recent 20 for base, 50 for deployable
aws ecr put-lifecycle-policy --repository-name bioindex-base --region "$REGION" \
    --lifecycle-policy-text '{"rules":[{"rulePriority":1,"description":"Keep last 20","selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":20},"action":{"type":"expire"}}]}' \
    >/dev/null
aws ecr put-lifecycle-policy --repository-name bioindex-deployable --region "$REGION" \
    --lifecycle-policy-text '{"rules":[{"rulePriority":1,"description":"Keep last 50","selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":50},"action":{"type":"expire"}}]}' \
    >/dev/null

echo "ECR repos and lifecycle policies in place."
