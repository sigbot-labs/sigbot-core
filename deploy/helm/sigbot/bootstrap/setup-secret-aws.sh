#!/usr/bin/env bash
# Store the canonical env payload in AWS Secrets Manager, then mirror it to K8s.
set -Eeuo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/setup-secret.sh"
parse_common_args "$@"
validate_secrets

: "${AWS_REGION:=ap-southeast-1}"
: "${AWS_PROFILE:=default}"
: "${AWS_SECRET_ID:=$SIGBOT_SECRET_NAME}"
: "${SIGBOT_VALUES_FILE:=${PWD}/sigbot-aws-values.yaml}"
if [[ "$SIGBOT_DRY_RUN" == true ]]; then
  log "would create/update AWS Secrets Manager secret ${AWS_SECRET_ID} in ${AWS_REGION}"
else
  require_command aws
  aws sts get-caller-identity --profile "$AWS_PROFILE" --region "$AWS_REGION" >/dev/null
  if aws secretsmanager describe-secret --secret-id "$AWS_SECRET_ID" --profile "$AWS_PROFILE" --region "$AWS_REGION" >/dev/null 2>&1; then
    aws secretsmanager put-secret-value --secret-id "$AWS_SECRET_ID" --secret-string "file://${SIGBOT_SECRET_FILE}" \
      --profile "$AWS_PROFILE" --region "$AWS_REGION" >/dev/null
  else
    aws secretsmanager create-secret --name "$AWS_SECRET_ID" --secret-string "file://${SIGBOT_SECRET_FILE}" \
      --profile "$AWS_PROFILE" --region "$AWS_REGION" >/dev/null
  fi
fi
sync_kubernetes_secrets
write_values_file "$SIGBOT_VALUES_FILE" aws "  aws:\n    region: ${AWS_REGION}\n    secret_name: ${AWS_SECRET_ID}\n"
