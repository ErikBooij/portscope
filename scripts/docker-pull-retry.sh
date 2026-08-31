#!/usr/bin/env bash
set -euo pipefail

image="${1:?usage: docker-pull-retry.sh IMAGE}"
attempts="${DOCKER_PULL_ATTEMPTS:-5}"
base_delay="${DOCKER_PULL_BASE_DELAY_SECONDS:-5}"

if docker image inspect "$image" >/dev/null 2>&1; then
  exit 0
fi

for ((attempt = 1; attempt <= attempts; attempt++)); do
  if docker pull "$image"; then
    exit 0
  fi
  if ((attempt == attempts)); then
    break
  fi
  delay=$((attempt * base_delay))
  echo "Docker pull for ${image} failed; retrying in ${delay}s (${attempt}/${attempts})" >&2
  sleep "$delay"
done

echo "Docker pull for ${image} failed after ${attempts} attempts" >&2
exit 1
