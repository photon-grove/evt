#!/usr/bin/env bash
set -euo pipefail

host="${POSTGRES_HOST:-localhost}"
port="${POSTGRES_PORT:-5432}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      host="$2"
      shift 2
      ;;
    --port)
      port="$2"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

# A dependency-free TCP probe, mirroring the curl-based emulator readiness check: it confirms a
# server is accepting connections without requiring the PostgreSQL client tools on the host.
if (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; then
  echo "PostgreSQL is reachable at ${host}:${port}"
  exit 0
fi

echo "PostgreSQL is not reachable at ${host}:${port}" >&2
echo "start a local server, then run: terraform -chdir=infra/local-postgres apply -auto-approve" >&2
exit 1
