#!/usr/bin/env bash
set -euo pipefail

LP_URL='https://launchpad.net/ubuntu/+archivemirrors'
SUITE='noble'          # change to your Ubuntu codename
TIMEOUT=0.2            # seconds per connect attempt
BEST_TIME=999999
BEST_URL=""

# 1) Fetch HTML
if command -v curl &>/dev/null; then
  HTML=$(curl -fsSL "$LP_URL")
else
  HTML=$(wget -qO- "$LP_URL")
fi

# 2) Extract only http://*.archive.ubuntu.com URLs
mapfile -t MIRRORS < <(
  grep -oE 'http://[A-Za-z0-9\.-]+\.archive\.ubuntu\.com(/[^\"]*)?' <<<"$HTML" \
    | sed 's#/$##' \
    | sort -u
)

# 3) For each, check security pocket then measure latency
for base in "${MIRRORS[@]}"; do
  # construct security-Release URL
  SEC_URL="${base}/ubuntu/dists/${SUITE}-security/Release"

  # does the security Release file exist?
  if ! curl -fsSL --max-time 2 -o /dev/null "$SEC_URL" &>/dev/null; then
    continue
  fi

  # if it does, strip host and path for TCP probe
  host=${base#*://}
  host=${host%%/*}

  start_ms=$(date +%s%3N)
  if timeout $TIMEOUT bash -c "exec 3<>/dev/tcp/$host/80"; then
    elapsed=$(( $(date +%s%3N) - start_ms ))
    exec 3<&-; exec 3>&-

    if (( elapsed < BEST_TIME )); then
      BEST_TIME=$elapsed
      BEST_URL=$base
    fi
  fi
done

if [[ -n $BEST_URL ]]; then
  echo "$BEST_URL"
else
  echo "Error: no mirror with security pocket reachable within ${TIMEOUT}s" >&2
  exit 1
fi
