#!/usr/bin/env bash
set -euo pipefail

versions=(3.13.7 4.0.9 4.1.8 4.2.9 4.3.5)
for version in "${versions[@]}"; do
  name="portscope-rabbit-${version//./-}-${RANDOM}"
  docker run -d --rm --name "$name" \
    -e RABBITMQ_DEFAULT_USER=portscope_upstream \
    -e RABBITMQ_DEFAULT_PASS=upstream-secret \
    -e RABBITMQ_DEFAULT_VHOST=/portscope \
    -p 127.0.0.1::5672 "rabbitmq:${version}" >/dev/null
  cleanup() { docker stop "$name" >/dev/null 2>&1 || true; }
  trap cleanup EXIT
  address=""
  for _ in $(seq 1 90); do
    port=$(docker port "$name" 5672/tcp 2>/dev/null | awk -F: 'NR == 1 { print $NF }' || true)
    if [[ -n "$port" ]] && docker exec --user rabbitmq "$name" rabbitmq-diagnostics -q check_protocol_listener amqp >/dev/null 2>&1; then
      address="127.0.0.1:${port}"
      break
    fi
    if [[ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null || true)" != "true" ]]; then
      docker logs "$name" 2>&1 || true
      exit 1
    fi
    sleep 1
  done
  if [[ -z "$address" ]]; then
    docker logs "$name"
    exit 1
  fi
  echo "RabbitMQ ${version} at ${address}"
  RABBIT_MATRIX_ADDR="$address" RABBIT_MATRIX_VERSION="$version" \
    GOCACHE="${GO_CACHE:-/tmp/portscope-go-cache}" \
    go test -tags=rabbitmatrix ./internal/proxy/rabbitadapter -run '^TestRealRabbitMQCompatibility$' -count=1
  cleanup
  trap - EXIT
done
