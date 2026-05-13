#!/usr/bin/env bash
# ECR helpers: image-existence checks, login, push.

# image_exists_in_ecr <repo> <tag>
# Returns 0 if the image exists, 1 if not.
image_exists_in_ecr() {
    local repo="$1"
    local tag="$2"
    aws ecr describe-images \
        --repository-name "$repo" \
        --image-ids "imageTag=$tag" \
        >/dev/null 2>&1
}

# _aws_account_id
# Echo the current AWS account ID, or fail loudly if credentials are unavailable.
_aws_account_id() {
    local account_id
    if ! account_id=$(aws sts get-caller-identity --query Account --output text 2>/dev/null); then
        echo "ERROR: could not determine AWS account ID. Check AWS credentials." >&2
        return 1
    fi
    if [[ -z "$account_id" ]]; then
        echo "ERROR: aws sts returned empty account ID." >&2
        return 1
    fi
    echo "$account_id"
}

# ecr_login
# Authenticates docker to ECR for the current AWS_REGION.
# Safe to call multiple times (no fast-path check; hits STS + ECR every time).
ecr_login() {
    local region="${AWS_REGION:-us-east-1}"
    local account_id
    account_id=$(_aws_account_id) || return 1

    local password
    if ! password=$(aws ecr get-login-password --region "$region" 2>/dev/null); then
        echo "ERROR: failed to get ECR login password (region=$region)." >&2
        return 1
    fi
    echo "$password" | docker login --username AWS --password-stdin \
        "${account_id}.dkr.ecr.${region}.amazonaws.com"
}

# ecr_registry_url
# Echo the ECR registry URL for the current account+region.
ecr_registry_url() {
    local region="${AWS_REGION:-us-east-1}"
    local account_id
    account_id=$(_aws_account_id) || return 1
    echo "${account_id}.dkr.ecr.${region}.amazonaws.com"
}
