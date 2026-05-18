# Plan B Operator Runbook

This document covers one-time setup and day-to-day operations for the consolidated bioindex deployment. See `docs/superpowers/specs/2026-05-13-bioindex-plan-b-deployment-design.md` for design context.

## First-time setup

### Prerequisites (account-wide, once)

1. **Install operator tools locally:**
   - Docker
   - AWS CLI v2 (`aws --version`)
   - Git
   - `jq`
   - `shellcheck`, `bats-core` for the shell tests (optional, for dev)
   - `gh` (GitHub CLI) for private repo work
   - GNU coreutils `timeout` (on macOS: `brew install coreutils`)

2. **Configure AWS credentials.** Operator role must allow ECR push, ECS service update, CloudFormation create/update, IAM passrole.

3. **Create the ECR repos (one-time, per account):**

   ```bash
   ./scripts/ecr-create-repos.sh
   ```

### Per env (qa, then prod)

1. **Create the Secrets Manager entry for the token signing key:**

   ```bash
   KEY_HEX=$(openssl rand -hex 32)
   aws secretsmanager create-secret \
     --name bioindex-token-signing-<env> \
     --secret-string "$KEY_HEX" \
     --description "HMAC signing key for bioindex continuation tokens (<env>)"
   ```

   Note the ARN for the parameters file.

2. **Identify VPC + subnet + SG IDs:**

   ```bash
   aws ec2 describe-vpcs --query 'Vpcs[*].[VpcId,Tags]' --output table
   aws ec2 describe-subnets --filters Name=vpc-id,Values=<VpcId> --query 'Subnets[*].[SubnetId,AvailabilityZone,Tags]' --output table
   aws ec2 describe-security-groups --filters Name=vpc-id,Values=<VpcId> --query 'SecurityGroups[*].[GroupId,GroupName]' --output table
   ```

3. **Fill `infra/parameters/<env>.json`** — replace every `REPLACE_ME_*` with the actual value.

4. **Create the stack:**

   ```bash
   ./scripts/infra-create.sh <env>
   ```

   Takes ~5 minutes.

5. **Add task → RDS ingress rules manually.** The stack does not manage RDS SG ingress (RdsSgIds is a list parameter; CFN can't iterate generically).

   ```bash
   TASK_SG=$(aws ec2 describe-security-groups \
     --filters Name=group-name,Values=bioindex-tasks-<env>-sg \
     --query 'SecurityGroups[0].GroupId' --output text)

   for rds_sg in <RDS_SG_ID_1> <RDS_SG_ID_2>; do
     aws ec2 authorize-security-group-ingress \
       --group-id "$rds_sg" \
       --protocol tcp --port 3306 \
       --source-group "$TASK_SG"
   done
   ```

6. **Build the base image (one-time per app code release):**

   ```bash
   ./scripts/build-base.sh --branch master
   ```

   Output prints the new SHA. Update `dig-bioindex-configs/BASE_IMAGE_SHA` and commit + push.

7. **First deploy:**

   ```bash
   ./scripts/deploy.sh <env> --branch main
   ```

   Wait for `services-stable`. Then verify from a host inside the VPC whose SG is `AlbIngressSgId` (a bastion, a test EC2, or via `aws ecs execute-command` into a running task):

   ```bash
   ALB=$(aws cloudformation describe-stacks --stack-name bioindex-<env> \
     --query 'Stacks[0].Outputs[?OutputKey==`AlbDnsName`].OutputValue' --output text)
   curl -fsS http://$ALB/ready | jq '.status'
   # Smoke-test one portal end-to-end (path-prefix routing):
   curl -fsS "http://$ALB/<portal>/api/bio/indexes" | jq '.count'
   ```

8. **Side-by-side validation.** Run representative queries against the new ALB and compare against the same queries on the existing per-portal EC2 hosts:

   - `count` / record shapes match.
   - p50/p95 latency is within tolerance (CloudWatch Logs Insights, see `docs/observability.md`).
   - Continuation tokens roundtrip across multiple tasks.

   The existing nginx config and client URLs are **not** changed. Cutover (whatever shape it takes) is a separate effort run once the parallel system is trusted.

## Configs repo bootstrap (one-time)

The `dig-bioindex-configs` private repo holds per-portal YAMLs.

1. Create the empty private repo:

   ```bash
   gh repo create broadinstitute/dig-bioindex-configs --private \
     --description "Per-portal configs and Dockerfile for the consolidated bioindex deployment"
   ```

2. Clone it locally:

   ```bash
   cd ~/code-repos/broad
   git clone git@github.com:broadinstitute/dig-bioindex-configs.git
   cd dig-bioindex-configs
   ```

3. Seed from this repo's `bio-configs-yaml/`:

   ```bash
   cp -r ../dig-bioindex/bio-configs-yaml/envs    ./envs
   cp -r ../dig-bioindex/bio-configs-yaml/portals ./portals
   mkdir -p schemas && touch schemas/.gitkeep
   ```

4. Add `prod.yaml` env defaults:

   ```bash
   cat > envs/prod.yaml <<'EOF'
   BIOINDEX_RESPONSE_LIMIT: 1048576
   BIOINDEX_RESPONSE_LIMIT_MAX: 104857600
   BIOINDEX_MATCH_LIMIT: 100
   EOF
   ```

5. Add `BASE_IMAGE_SHA`, `Dockerfile`, `README.md` (see plan for full contents).

6. Initial commit + push:

   ```bash
   git add .
   git commit -m "feat: initial configs repo bootstrap"
   git push -u origin main
   ```

## Day-to-day commands

| What | Command |
|---|---|
| Deploy current main to QA | `./scripts/deploy.sh qa --branch main` |
| Deploy a specific tag to Prod | `./scripts/deploy.sh prod --tag v2026.05.13` |
| Deploy from a local clone | `./scripts/deploy.sh qa --local ~/dev/dig-bioindex-configs` |
| Rollback Prod to previous SHA | `./scripts/deploy.sh prod --sha <prev>` |
| Build a new base image | `./scripts/build-base.sh --branch master` |
| Update infrastructure | `./scripts/infra-update.sh <env>` |
| Debug a running task | `aws ecs execute-command --cluster bioindex-<env> --task <id> --container bioindex --command /bin/bash --interactive` |

## CloudWatch queries

See `docs/observability.md` (Plan A) for Logs Insights queries.

## Scaling adjustments

To change `MinTasks`, edit `infra/parameters/<env>.json` and run `./scripts/infra-update.sh <env>`. CFN updates the auto-scaling target. The service drains gracefully.

## Troubleshooting

**`build-base.sh` fails with "HEAD not on origin"**
→ Push your local commits before running `--local`, or use `--branch master`.

**`deploy.sh` fails with "bioindex-base:<sha> does not exist in ECR"**
→ Run `./scripts/build-base.sh --sha <sha>`.

**`services-stable` times out**
→ Check CloudWatch Logs for the new tasks. Likely a startup error. Roll back with `./scripts/deploy.sh <env> --sha <prev>`.

**ECS Exec returns "session not started"**
→ Confirm `EnableExecuteCommand: true` on the service and that the task role allows `ssmmessages:*` (both in the stack template).

**Task can't reach RDS**
→ Did you add the task SG to the RDS SG ingress? See pre-stack setup step 5.
