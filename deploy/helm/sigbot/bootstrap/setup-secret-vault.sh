#!/usr/bin/env bash
# Write the canonical env payload to Vault KV v2 with a read-only workload role.
set -Eeuo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/setup-secret.sh"
parse_common_args "$@"
validate_secrets

: "${VAULT_KV_MOUNT:=kv}"
: "${VAULT_SECRET_PATH:=sigbot/runtime}"
: "${VAULT_POLICY_NAME:=sigbot-runtime-read}"
: "${VAULT_ROLE_NAME:=sigbot}"
: "${VAULT_AUTH_MOUNT:=kubernetes}"
: "${SIGBOT_VALUES_FILE:=${PWD}/sigbot-vault-values.yaml}"
if [[ "$SIGBOT_DRY_RUN" == true ]]; then
  log "would write Vault ${VAULT_KV_MOUNT}/${VAULT_SECRET_PATH} and role ${VAULT_ROLE_NAME}"
else
  require_command vault
  vault status >/dev/null
  vault kv put -mount "$VAULT_KV_MOUNT" "$VAULT_SECRET_PATH" "env=@${SIGBOT_SECRET_FILE}" >/dev/null
  vault policy write "$VAULT_POLICY_NAME" - <<EOF
path "${VAULT_KV_MOUNT}/data/${VAULT_SECRET_PATH}" { capabilities = ["read"] }
EOF
  vault write "auth/${VAULT_AUTH_MOUNT}/role/${VAULT_ROLE_NAME}" \
    bound_service_account_names="${SIGBOT_RELEASE}" \
    bound_service_account_namespaces="${SIGBOT_NAMESPACE}" \
    policies="${VAULT_POLICY_NAME}" ttl=1h >/dev/null
fi
sync_kubernetes_secrets
write_values_file "$SIGBOT_VALUES_FILE" vault "  vault:\n    role: ${VAULT_ROLE_NAME}\n    secret_path: ${VAULT_KV_MOUNT}/data/${VAULT_SECRET_PATH}\n"
