#!/bin/bash
set -euo pipefail

usage() {
  echo "Usage: $0 <url>" >&2
  exit 1
}

if [[ $# -lt 1 ]]; then
  usage
fi

url="$1"

echo "Verifying download URL: $url"

# Verify with curl retry
if curl --silent --head --fail --max-time 60 --retry 3 --retry-delay 20 "$url" >/dev/null; then
  echo "Download link is valid"
else
  echo "Download link is not valid"
  exit 1
fi
