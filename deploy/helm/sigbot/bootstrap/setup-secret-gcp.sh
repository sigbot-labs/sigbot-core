#!/usr/bin/env bash
# Store the canonical env payload in GCP Secret Manager, then mirror it to K8s.
set -Eeuo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/setup-secret.sh"
parse_common_args "$@"
validate_secrets

: "${GCP_PROJECT_ID:?set GCP_PROJECT_ID}"
: "${GCP_SECRET_ID:=$SIGBOT_SECRET_NAME}"
: "${SIGBOT_VALUES_FILE:=${PWD}/sigbot-gcp-values.yaml}"
if [[ "$SIGBOT_DRY_RUN" == true ]]; then
  log "would create/update GCP Secret Manager secret ${GCP_PROJECT_ID}/${GCP_SECRET_ID}"
else
  require_command gcloud
  gcloud services enable secretmanager.googleapis.com --project "$GCP_PROJECT_ID" >/dev/null
  if ! gcloud secrets describe "$GCP_SECRET_ID" --project "$GCP_PROJECT_ID" >/dev/null 2>&1; then
    gcloud secrets create "$GCP_SECRET_ID" --replication-policy automatic --project "$GCP_PROJECT_ID" >/dev/null
  fi
  gcloud secrets versions add "$GCP_SECRET_ID" --data-file "$SIGBOT_SECRET_FILE" --project "$GCP_PROJECT_ID" >/dev/null
fi
sync_kubernetes_secrets
write_values_file "$SIGBOT_VALUES_FILE" gcp "  gcp:\n    project_id: ${GCP_PROJECT_ID}\n    secret_id: ${GCP_SECRET_ID}\n"
