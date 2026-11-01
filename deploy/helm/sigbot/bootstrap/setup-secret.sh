#!/usr/bin/env bash
# Shared CLI, validation and Kubernetes mirroring for all Sigbot secret stores.
set -Eeuo pipefail

readonly BOOTSTRAP_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly SECRET_TEMPLATE="${BOOTSTRAP_DIR}/sigbot-secrets.env.example"

SIGBOT_RELEASE="${SIGBOT_RELEASE:-sigbot}"
SIGBOT_NAMESPACE="${SIGBOT_NAMESPACE:-sigbot}"
SIGBOT_SECRET_NAME="${SIGBOT_SECRET_NAME:-sigbot-runtime}"
SIGBOT_SECRET_FILE="${SIGBOT_SECRET_FILE:-${PWD}/sigbot-secrets.env}"
SIGBOT_WORKER_SECRET_FILE="${SIGBOT_WORKER_SECRET_FILE:-}"
SIGBOT_AUTHGUARD_SECRET_FILE="${SIGBOT_AUTHGUARD_SECRET_FILE:-}"
SIGBOT_SKIP_KUBERNETES_MIRROR="${SIGBOT_SKIP_KUBERNETES_MIRROR:-false}"
SIGBOT_DRY_RUN=false

log() { printf '%s\n' "[sigbot-bootstrap] $*" >&2; }
die() { log "error: $*"; exit 1; }
require_command() { command -v "$1" >/dev/null 2>&1 || die "required command is missing: $1"; }
need_value() { [[ $# -eq 2 && -n "$2" && "$2" != --* ]] || die "$1 requires a value"; }

print_common_options() {
  cat <<'EOF'
Common options:
  --release NAME                Helm release (default: sigbot).
  --namespace NAME              Kubernetes namespace (default: sigbot).
  --secret-name NAME            Runtime Secret name (default: sigbot-runtime).
  --secret-file PATH            Filled Sigbot env-file.
  --worker-secret-file PATH     Optional controller worker credential env-file.
  --authguard-secret-file PATH  Optional AuthGuard credential env-file.
  --skip-kubernetes-mirror      Do not mirror local files to Kubernetes Secrets.
  --dry-run                     Validate and print intended targets only.
EOF
}

parse_common_args() {
  while (($#)); do
    case "$1" in
      --release) need_value "$1" "${2:-}"; SIGBOT_RELEASE="$2"; shift 2 ;;
      --namespace) need_value "$1" "${2:-}"; SIGBOT_NAMESPACE="$2"; shift 2 ;;
      --secret-name) need_value "$1" "${2:-}"; SIGBOT_SECRET_NAME="$2"; shift 2 ;;
      --secret-file) need_value "$1" "${2:-}"; SIGBOT_SECRET_FILE="$2"; shift 2 ;;
      --worker-secret-file) need_value "$1" "${2:-}"; SIGBOT_WORKER_SECRET_FILE="$2"; shift 2 ;;
      --authguard-secret-file) need_value "$1" "${2:-}"; SIGBOT_AUTHGUARD_SECRET_FILE="$2"; shift 2 ;;
      --skip-kubernetes-mirror) SIGBOT_SKIP_KUBERNETES_MIRROR=true; shift ;;
      --dry-run) SIGBOT_DRY_RUN=true; shift ;;
      -h|--help) print_common_options; exit 0 ;;
      *) die "unknown option: $1" ;;
    esac
  done
}

validate_env_file() {
  local path="$1"
  [[ -s "$path" ]] || die "secret file is missing or empty: $path; start with: cp ${SECRET_TEMPLATE} $path"
  grep -nE '^[[:space:]]*[^#].*=<[^>]+>' "$path" && die "replace every active placeholder in $path"
  local key value
  while IFS='=' read -r key value || [[ -n "$key" ]]; do
    [[ -z "$key" || "$key" == \#* ]] && continue
    [[ "$key" =~ ^[A-Z][A-Z0-9_]*$ && -n "$value" ]] || die "invalid key or empty value in $path"
  done <"$path"
  [[ "$(grep -Ev '^[[:space:]]*(#|$)' "$path" | cut -d= -f1 | sort | uniq -d | wc -l)" -eq 0 ]] \
    || die "duplicate secret keys in $path"
}

validate_secrets() {
  validate_env_file "$SIGBOT_SECRET_FILE"
  for key in POSTGRES_PASSWORD REDIS_PASSWORD; do
    grep -q "^${key}=" "$SIGBOT_SECRET_FILE" || die "required key is missing: $key"
  done
  [[ -z "$SIGBOT_WORKER_SECRET_FILE" ]] || validate_env_file "$SIGBOT_WORKER_SECRET_FILE"
  [[ -z "$SIGBOT_AUTHGUARD_SECRET_FILE" ]] || validate_env_file "$SIGBOT_AUTHGUARD_SECRET_FILE"
}

apply_secret() {
  local name="$1" path="$2"
  kubectl -n "$SIGBOT_NAMESPACE" create secret generic "$name" --from-env-file="$path" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
}

sync_kubernetes_secrets() {
  [[ "$SIGBOT_SKIP_KUBERNETES_MIRROR" == false ]] || { log "Kubernetes mirror skipped"; return; }
  if [[ "$SIGBOT_DRY_RUN" == true ]]; then
    log "would mirror runtime credentials to ${SIGBOT_NAMESPACE}/${SIGBOT_SECRET_NAME}"
    return
  fi
  require_command kubectl
  kubectl create namespace "$SIGBOT_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  apply_secret "$SIGBOT_SECRET_NAME" "$SIGBOT_SECRET_FILE"
  [[ -z "$SIGBOT_WORKER_SECRET_FILE" ]] || apply_secret sigbot-workers "$SIGBOT_WORKER_SECRET_FILE"
  [[ -z "$SIGBOT_AUTHGUARD_SECRET_FILE" ]] || apply_secret authguard-secrets "$SIGBOT_AUTHGUARD_SECRET_FILE"
  log "Kubernetes Secret mirror is ready"
}

write_values_file() {
  local path="$1" provider="$2" extra="${3:-}"
  [[ "$SIGBOT_DRY_RUN" == false ]] || { log "would write $path"; return; }
  umask 077
  printf 'secrets:\n  provider: %s\n  existingSecret: %s\n%s' "$provider" "$SIGBOT_SECRET_NAME" "$extra" >"$path"
  log "wrote non-secret Helm values: $path"
}
