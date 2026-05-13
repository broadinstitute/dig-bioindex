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

# ecr_login
# Authenticates docker to ECR for the current AWS_REGION.
# Idempotent — safe to call multiple times.
ecr_login() {
    local region="${AWS_REGION:-us-east-1}"
    local account_id
    account_id=$(aws sts get-caller-identity --query Account --output text)
    aws ecr get-login-password --region "$region" \
        | docker login --username AWS --password-stdin \
            "${account_id}.dkr.ecr.${region}.amazonaws.com"
}

# ecr_registry_url
# Echo the ECR registry URL for the current account+region.
ecr_registry_url() {
    local region="${AWS_REGION:-us-east-1}"
    local account_id
    account_id=$(aws sts get-caller-identity --query Account --output text)
    echo "${account_id}.dkr.ecr.${region}.amazonaws.com"
}
