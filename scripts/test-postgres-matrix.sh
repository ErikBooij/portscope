#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

versions=("$@")
if [[ $# -eq 0 ]]; then
  versions=(14 15 16 17 18)
fi

matrix_password="portscope-matrix-secret"
matrix_database="portscope_compat"
current_container=""

cleanup() {
  if [[ -n "$current_container" ]]; then
    docker rm -f "$current_container" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

for version in "${versions[@]}"; do
  current_container="portscope-postgres-${version}-$$"
  echo "Testing PostgreSQL ${version}"
  "$script_dir/docker-pull-retry.sh" "postgres:${version}"
  docker run -d --name "$current_container" \
    -e POSTGRES_PASSWORD="$matrix_password" \
    -e POSTGRES_DB="$matrix_database" \
    -p 127.0.0.1::5432 "postgres:${version}" >/dev/null

  address="$(docker port "$current_container" 5432/tcp | tail -1)"
  ready=false
  for _ in $(seq 1 60); do
    if docker exec "$current_container" pg_isready -h127.0.0.1 -U postgres -d "$matrix_database" >/dev/null 2>&1; then
      ready=true
      break
    fi
    sleep 1
  done
  if [[ "$ready" != true ]]; then
    docker logs "$current_container"
    exit 1
  fi

  reported_version="$(docker exec "$current_container" psql -h127.0.0.1 -U postgres -d "$matrix_database" -Atc "SHOW server_version" 2>/dev/null)"
  POSTGRES_MATRIX_ADDR="$address" \
  POSTGRES_MATRIX_PASSWORD="$matrix_password" \
  POSTGRES_MATRIX_DATABASE="$matrix_database" \
  POSTGRES_MATRIX_VERSION="$reported_version" \
  GOCACHE="${GOCACHE:-/tmp/portscope-go-cache}" \
    go test -tags=postgresmatrix ./internal/proxy/postgresadapter -run '^TestRealPostgresCompatibility$' -count=1

  docker rm -f "$current_container" >/dev/null
  current_container=""
done
