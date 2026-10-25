#!/usr/bin/env bash
# Colvenna Isle: one command from a clean checkout to an installed (or upgraded) environment.
#
#   ./install.sh fresh   --environment <name> --account <12-digit account> [options] [-- <install.py options>]
#   ./install.sh upgrade --environment <name> --account <12-digit account> [options] [-- <install.py options>]
#
# Steps (each one is the documented tool; see docs/installation.md):
#   1. preflight: ARM64 host, Python 3.12, Docker, Node.js, git, clean checkout, instance-role credentials in the account
#   2. build environment: <work-dir>/venv from the hash-locked requirement files
#   3. native storage image: present locally, or pulled from the account's ECR by its locked digest
#   4. release: scripts/build_product.py into <work-dir>/release-<commit> (reused when it already passed)
#   5. install: scripts/aws/install.py fresh|upgrade with a new receipt directory under <work-dir>/receipts
#   6. optional: provision a space and issue its first administrator key (--space with --token-output)
#   7. print the API and web URLs
#
# Options:
#   --environment NAME     environment name (2-12 lowercase letters or digits, starting with a letter); required
#   --account ID           target AWS account (defaults to $COLVENNA_ACCOUNT); must match the instance role's account
#   --region NAME          target commercial AWS region (or COLVENNA_REGION / AWS_REGION); required
#   --work-dir DIR         build environment, release and receipts (default: ../colvenna-isle-work); outside the repo
#   --steps LIST           resume a failed install from the given install.py steps (after reconciling the failed one)
#   --space ID             after installing: provision this space and issue its first administrator key
#   --token-output FILE    new private file for that key (required with --space)
#   --expires-at TIME      expiry of that key (default: 30 days from now, UTC)
#   --check                run the preflight checks only
#   -h, --help             show this help
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGION="${COLVENNA_REGION:-${AWS_REGION:-${AWS_DEFAULT_REGION:-}}}"
PLAN="" ENVIRONMENT="" ACCOUNT="${COLVENNA_ACCOUNT:-}" WORK="" STEPS="" SPACE="" TOKEN_OUTPUT="" EXPIRES_AT="" CHECK_ONLY=0
PASSTHROUGH=()

usage() { sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | sed '$d' | sed 's/^# \{0,1\}//'; }
fail() { echo "install.sh: $*" >&2; exit 1; }
step() { echo; echo "==> $*"; }

[ $# -gt 0 ] || { usage; exit 1; }
case "$1" in
  fresh|upgrade) PLAN=$1; shift ;;
  -h|--help) usage; exit 0 ;;
  *) fail "the first argument must be fresh or upgrade (see --help)" ;;
esac
while [ $# -gt 0 ]; do
  case "$1" in
    --environment) ENVIRONMENT=${2:?}; shift 2 ;;
    --account) ACCOUNT=${2:?}; shift 2 ;;
    --region) REGION=${2:?}; shift 2 ;;
    --work-dir) WORK=${2:?}; shift 2 ;;
    --steps) STEPS=${2:?}; shift 2 ;;
    --space) SPACE=${2:?}; shift 2 ;;
    --token-output) TOKEN_OUTPUT=${2:?}; shift 2 ;;
    --expires-at) EXPIRES_AT=${2:?}; shift 2 ;;
    --check) CHECK_ONLY=1; shift ;;
    -h|--help) usage; exit 0 ;;
    --) shift; PASSTHROUGH=("$@"); break ;;
    *) fail "unknown option $1 (see --help)" ;;
  esac
done

[[ "$ENVIRONMENT" =~ ^[a-z][a-z0-9]{1,11}$ ]] || fail "--environment must be 2-12 lowercase letters or digits, starting with a letter"
case "$ENVIRONMENT" in env|core|control|builder) fail "--environment $ENVIRONMENT is reserved" ;; esac
[[ "$ACCOUNT" =~ ^[0-9]{12}$ ]] || fail "--account (or COLVENNA_ACCOUNT) must be the 12-digit target account"
[[ "$REGION" =~ ^[a-z]{2}(-[a-z]+)+-[1-9][0-9]*$ ]] || fail "set --region, COLVENNA_REGION or AWS_REGION"
case "$REGION" in cn-*|us-gov-*|us-iso*|eu-isoe-*|eusc-*) fail "only the commercial AWS partition is supported" ;; esac
for configured in "${COLVENNA_REGION:-}" "${AWS_REGION:-}" "${AWS_DEFAULT_REGION:-}"; do
  [ -z "$configured" ] || [ "$configured" = "$REGION" ] || fail "configured AWS regions disagree with --region"
done
if [ -n "$SPACE" ] && [ -z "$TOKEN_OUTPUT" ]; then fail "--space needs --token-output"; fi
WORK="$(mkdir -p "${WORK:-$ROOT/../colvenna-isle-work}" && cd "${WORK:-$ROOT/../colvenna-isle-work}" && pwd)"
case "$WORK/" in "$ROOT"/*) fail "--work-dir must be outside the repository" ;; esac

# The operator tools accept only the EC2 instance-role credential chain, never a profile or static keys.
AWS_ENV=(/usr/bin/env -u AWS_PROFILE -u AWS_DEFAULT_PROFILE -u AWS_ACCESS_KEY_ID -u AWS_SECRET_ACCESS_KEY
         -u AWS_SESSION_TOKEN "COLVENNA_REGION=$REGION" "AWS_REGION=$REGION" "AWS_DEFAULT_REGION=$REGION"
         "COLVENNA_ACCOUNT=$ACCOUNT" "COLVENNA_ENVIRONMENT=$ENVIRONMENT")

step "Preflight"
[ "$(uname -m)" = aarch64 ] || fail "the deployment host must be ARM64 (aarch64); found $(uname -m)"
command -v python3.12 >/dev/null || command -v python3 >/dev/null || fail "Python 3.12 is required"
PYTHON=$(command -v python3.12 || command -v python3)
"$PYTHON" -c 'import sys; sys.exit(sys.version_info[:2] != (3, 12))' || fail "Python 3.12 is required ($PYTHON)"
command -v docker >/dev/null || fail "Docker is required"
docker info >/dev/null 2>&1 || fail "Docker is not usable by this user"
command -v node >/dev/null || fail "Node.js is required"
command -v aws >/dev/null || fail "AWS CLI v2 is required"
git -C "$ROOT" rev-parse --git-dir >/dev/null 2>&1 || fail "run from a git checkout of colvenna-isle"
[ -z "$(git -C "$ROOT" status --porcelain)" ] || fail "the checkout has uncommitted changes; install only a committed source"
COMMIT=$(git -C "$ROOT" rev-parse HEAD)
CALLER=$("${AWS_ENV[@]}" aws sts get-caller-identity --output json) || fail "no AWS credentials from the instance role"
CALLER_ACCOUNT=$(echo "$CALLER" | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["Account"])')
CALLER_ARN=$(echo "$CALLER" | "$PYTHON" -c 'import json,sys; print(json.load(sys.stdin)["Arn"])')
[ "$CALLER_ACCOUNT" = "$ACCOUNT" ] || fail "the instance role is in another account than --account"
[[ "$CALLER_ARN" == *":assumed-role/"* ]] || fail "use the EC2 instance role, not user credentials"
echo "source $COMMIT, environment $ENVIRONMENT, region $REGION, plan $PLAN"
if [ "$CHECK_ONLY" = 1 ]; then echo "preflight passed"; exit 0; fi

step "Build environment ($WORK/venv)"
VENV="$WORK/venv"
if [ ! -x "$VENV/bin/python" ]; then "$PYTHON" -m venv "$VENV"; fi
"$VENV/bin/python" -m pip install -q --require-hashes --only-binary=:all: \
  -r "$ROOT/runtime/contracts-requirements.lock" -r "$ROOT/runtime/aws-requirements.lock" \
  -r "$ROOT/runtime/build-requirements.lock" -r "$ROOT/examples/orders/requirements.lock"

step "Native storage image"
read -r REPOSITORY DIGEST < <("$VENV/bin/python" -c '
import json, sys
lock = json.load(open(sys.argv[1]))
print(lock["storage_repository"], lock["storage_digest"])' "$ROOT/runtime/writer-lock.json")
REGISTRY="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"
IMAGE="$REGISTRY/$REPOSITORY@$DIGEST"
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  "${AWS_ENV[@]}" aws ecr get-login-password | docker login --username AWS --password-stdin "$REGISTRY" >/dev/null \
    || fail "cannot log in to $REGISTRY"
  docker pull "$IMAGE" >/dev/null || fail "the locked native storage image is not in this account's ECR;
  build and publish it first (docs/installation.md, section 4)"
fi
echo "$IMAGE"

RELEASE="$WORK/release-${COMMIT:0:12}"
step "Release ($RELEASE)"
release_ok() {
  "$VENV/bin/python" -c '
import json, sys
manifest = json.load(open(sys.argv[1] + "/manifest.json"))
sys.exit(manifest.get("status") != "PASS" or manifest.get("source_commit") != sys.argv[2])' "$RELEASE" "$COMMIT"
}
if [ -f "$RELEASE/manifest.json" ] && release_ok; then
  echo "reusing the passing release for $COMMIT"
else
  [ ! -e "$RELEASE" ] || mv "$RELEASE" "$RELEASE.failed-$(date -u +%Y%m%dT%H%M%SZ)"
  (cd "$ROOT" && "${AWS_ENV[@]}" "$VENV/bin/python" scripts/build_product.py --output "$RELEASE") \
    || fail "the release build failed; see $RELEASE/*.log"
  release_ok || fail "the release manifest is not PASS for $COMMIT"
fi

step "Install ($PLAN)"
mkdir -p "$WORK/receipts"
RECEIPTS="$WORK/receipts/install-$ENVIRONMENT-${COMMIT:0:12}-$PLAN-$(date -u +%Y%m%dT%H%M%SZ)"
ARGS=("$PLAN" --release "$RELEASE" --output "$RECEIPTS")
[ -z "$STEPS" ] || ARGS+=(--steps "$STEPS")
(cd "$ROOT" && "${AWS_ENV[@]}" "$VENV/bin/python" scripts/aws/install.py "${ARGS[@]}" "${PASSTHROUGH[@]}") \
  || fail "install stopped; reconcile the failed step in $RECEIPTS, then resume with --steps (docs/installation.md, 6.1)"

if [ -n "$SPACE" ]; then
  step "Space $SPACE and its first administrator key"
  EXPIRES_AT=${EXPIRES_AT:-$(date -u -d '+30 days' +%Y-%m-%dT%H:%M:%SZ)}
  (cd "$ROOT" && "${AWS_ENV[@]}" "$VENV/bin/python" scripts/aws/control.py provision --space "$SPACE" \
    --output "$WORK/receipts/provision-$ENVIRONMENT-$SPACE-$(date -u +%Y%m%dT%H%M%SZ)") || fail "provisioning failed"
  (cd "$ROOT" && "${AWS_ENV[@]}" "$VENV/bin/python" scripts/aws/control.py bootstrap-key --space "$SPACE" \
    --expires-at "$EXPIRES_AT" --token-output "$TOKEN_OUTPUT" \
    --output "$WORK/receipts/bootstrap-$ENVIRONMENT-$SPACE-$(date -u +%Y%m%dT%H%M%SZ)") || fail "key issuance failed"
  echo "administrator key written to $TOKEN_OUTPUT (keep it private)"
fi

step "Endpoints"
"${AWS_ENV[@]}" aws cloudformation describe-stacks --stack-name "colvenna-isle-$ENVIRONMENT-core" \
  --query "Stacks[0].Outputs[?OutputKey=='ApiUrl' || OutputKey=='WebUrl'].[OutputKey,OutputValue]" --output text
echo
echo "Query Editor: <WebUrl>/   Admin Console: <WebUrl>/admin.html   Next: examples/quickstart/README.md"
