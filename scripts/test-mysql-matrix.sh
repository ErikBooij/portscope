#!/usr/bin/env bash
set -euo pipefail

versions=("$@")
if [[ $# -eq 0 ]]; then
  versions=(5.6 5.7 8.0 8.4 9.7 26.7)
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
  normalized="${version//./-}"
  current_container="portscope-mysql-${normalized}-$$"
  platform_args=()
  if [[ "$(uname -m)" == "arm64" && ( "$version" == "5.6" || "$version" == "5.7" ) ]]; then
    platform_args=(--platform linux/amd64)
  fi

  echo "Testing MySQL ${version}"
  docker run -d --name "$current_container" "${platform_args[@]}" \
    -e MYSQL_ROOT_PASSWORD="$matrix_password" \
    -e MYSQL_ROOT_HOST=% \
    -e MYSQL_DATABASE="$matrix_database" \
    -p 127.0.0.1::3306 "mysql:${version}" >/dev/null

  address="$(docker port "$current_container" 3306/tcp | tail -1)"
  ready=false
  for _ in $(seq 1 90); do
    if docker exec "$current_container" mysqladmin ping -h127.0.0.1 -uroot -p"$matrix_password" --silent >/dev/null 2>&1; then
      ready=true
      break
    fi
    sleep 2
  done
  if [[ "$ready" != true ]]; then
    docker logs "$current_container"
    exit 1
  fi

  reported_version="$(docker exec "$current_container" mysql -N -B -h127.0.0.1 -uroot -p"$matrix_password" -e 'SELECT VERSION()' 2>/dev/null)"
  MYSQL_MATRIX_ADDR="$address" \
  MYSQL_MATRIX_PASSWORD="$matrix_password" \
  MYSQL_MATRIX_DATABASE="$matrix_database" \
  MYSQL_MATRIX_VERSION="$reported_version" \
  GOCACHE="${GOCACHE:-/tmp/portscope-go-cache}" \
    go test -tags=mysqlmatrix ./internal/proxy/mysqladapter -run '^TestRealMySQLCompatibility$' -count=1

  docker rm -f "$current_container" >/dev/null
  current_container=""
done
