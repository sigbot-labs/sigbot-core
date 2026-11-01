#!/usr/bin/env bash
set -Eeuo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/setup-secret.sh"
parse_common_args "$@"
validate_secrets
[[ "$SIGBOT_SKIP_KUBERNETES_MIRROR" == false ]] || die "Kubernetes bootstrap cannot skip its target"
sync_kubernetes_secrets
