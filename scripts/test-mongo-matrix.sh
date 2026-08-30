#!/usr/bin/env bash
set -euo pipefail

matrix=("6.0.28" "7.0.40" "8.0.29" "8.3.8")
if [[ -n "${MONGO_MATRIX_VERSIONS:-}" ]]; then
  read -r -a matrix <<<"$MONGO_MATRIX_VERSIONS"
fi
current_container=""

cleanup() {
  if [[ -n "$current_container" ]]; then
    docker rm -f "$current_container" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

for version in "${matrix[@]}"; do
  image="mongo:${version}"
  current_container="portscope-mongo-${version//./-}-$$"
  echo "Testing MongoDB ${version}"
  docker run -d --name "$current_container" \
    -e MONGO_INITDB_ROOT_USERNAME=portscope_upstream \
    -e MONGO_INITDB_ROOT_PASSWORD=upstream-secret \
    -p 127.0.0.1::27017 "$image" >/dev/null
  address="$(docker port "$current_container" 27017/tcp | tail -1)"
  ready=false
  for _ in $(seq 1 90); do
    if docker exec "$current_container" mongosh --quiet --username portscope_upstream --password upstream-secret --authenticationDatabase admin --eval 'db.adminCommand({ping:1}).ok' 2>/dev/null | grep -q 1; then
      ready=true
      break
    fi
    sleep 1
  done
  if [[ "$ready" != true ]]; then
    docker logs "$current_container"
    exit 1
  fi
  MONGO_MATRIX_ADDR="$address" MONGO_MATRIX_VERSION="$version" GOCACHE="${GOCACHE:-/tmp/portscope-go-cache}" \
    go test -tags=mongomatrix ./internal/proxy/mongoadapter -run '^TestRealMongoCompatibility$' -count=1
  docker rm -f "$current_container" >/dev/null
  current_container=""
  docker image rm "$image" >/dev/null
done
