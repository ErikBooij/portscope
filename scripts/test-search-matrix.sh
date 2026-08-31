#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

matrix=(
  "elasticsearch|8.19.20|docker.elastic.co/elasticsearch/elasticsearch:8.19.20"
  "elasticsearch|9.5.2|docker.elastic.co/elasticsearch/elasticsearch:9.5.2"
  "opensearch|2.19.6|opensearchproject/opensearch:2.19.6"
  "opensearch|3.8.0|opensearchproject/opensearch:3.8.0"
)
current_container=""

cleanup() {
  if [[ -n "$current_container" ]]; then
    docker rm -f "$current_container" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

for entry in "${matrix[@]}"; do
  IFS='|' read -r product version image <<<"$entry"
  current_container="portscope-search-${product}-${version//./-}-$$"
  echo "Testing ${product} ${version}"
  product_options=()
  if [[ "$product" == "elasticsearch" ]]; then
    product_options+=("-e" "xpack.security.enabled=false")
  else
    product_options+=("-e" "DISABLE_SECURITY_PLUGIN=true")
  fi
  "$script_dir/docker-pull-retry.sh" "$image"
  docker run -d --name "$current_container" \
    -e discovery.type=single-node \
    -e cluster.routing.allocation.disk.threshold_enabled=false \
    -e ES_JAVA_OPTS="-Xms512m -Xmx512m" \
    -e OPENSEARCH_JAVA_OPTS="-Xms512m -Xmx512m" \
    "${product_options[@]}" \
    -p 127.0.0.1::9200 "$image" >/dev/null
  address="$(docker port "$current_container" 9200/tcp | tail -1)"
  ready=false
  for _ in $(seq 1 120); do
    if curl -fsS "http://${address}/" >/dev/null 2>&1; then
      ready=true
      break
    fi
    sleep 1
  done
  if [[ "$ready" != true ]]; then
    docker logs "$current_container"
    exit 1
  fi
  if ! curl -fsS "http://${address}/_cluster/health?wait_for_status=yellow&timeout=120s" >/dev/null; then
    docker logs "$current_container"
    exit 1
  fi
  SEARCH_MATRIX_URL="http://${address}" SEARCH_MATRIX_VERSION="$version" GOCACHE="${GOCACHE:-/tmp/portscope-go-cache}" \
    go test -tags=searchmatrix ./internal/proxy/httpadapter -run '^TestRealSearchCompatibility$' -count=1
  docker rm -f "$current_container" >/dev/null
  current_container=""
  docker image rm "$image" >/dev/null
done
