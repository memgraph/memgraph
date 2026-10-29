#!/bin/bash
# Print the latest released (non-rc) memgraph version, without the leading "v".
# Authenticates with $GITHUB_TOKEN when set: unauthenticated calls share a
# 60 req/h per-IP limit that hosted runners regularly exhaust. Retries a few
# times and exits non-zero rather than printing an empty version.
set -euo pipefail

attempts=5
auth=()
if [[ -n "${GITHUB_TOKEN:-}" ]]; then
  auth=(-H "Authorization: Bearer $GITHUB_TOKEN")
fi

for i in $(seq 1 "$attempts"); do
  if response="$(curl -fsS "${auth[@]}" -H "Accept: application/vnd.github+json" \
      "https://api.github.com/repos/memgraph/memgraph/tags?per_page=100")" &&
    latest="$(python3 -c "
import sys, json
tags = json.load(sys.stdin)
print(next(tag['name'][1:] for tag in tags if 'rc' not in tag['name']))
" <<<"$response")" &&
    [[ -n "$latest" ]]; then
    echo "$latest"
    exit 0
  fi
  echo "Failed to get latest tag (attempt $i/$attempts)" >&2
  if (( i < attempts )); then
    sleep $((i * 5))
  fi
done

echo "Error: could not determine latest memgraph tag after $attempts attempts" >&2
exit 1
