#!/usr/bin/env bash
set -euo pipefail

# Decide whether a build should use the custom APT mirror kept in this
# directory, and print "true" or "false".
#
# The ci.sources files here point at the Hetzner mirror, which only answers
# from inside the Hetzner network. On runners hosted anywhere else every
# apt-get in the build container stalls until it times out, so those machines
# have to fall back to the distro's default sources.
#
# "true" is printed only when the build targets the release those files pin
# (noble, i.e. ubuntu-24.04) and every mirror URI in them answers a HEAD for
# its Release file within a couple of seconds.
#
# An unreachable mirror is an answer, not an error: the script still exits 0
# and prints "false", so callers can use it inline. Only a usage error (bad
# flag, missing sources file) exits non-zero.
#
#   USE_CUSTOM_MIRROR="$(tools/ci/ubuntu-mirrors/use_custom_mirror.sh --os "$OS" --arch "$ARCH")"
#
# Set MG_USE_CUSTOM_MIRROR=true|false to skip the probe and force the answer.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# The only distro the ci.sources files describe; keep in step with their Suites.
SUPPORTED_OS="ubuntu-24.04"
CONNECT_TIMEOUT=1
MAX_TIME=2
# One retry: the timeouts are tight enough that a single hiccup shouldn't cost
# an in-network runner its mirror.
ATTEMPTS=2

print_help() {
  cat <<'EOF'
Usage: use_custom_mirror.sh --os OS --arch ARCH

Prints "true" if the custom APT mirror in tools/ci/ubuntu-mirrors is usable for
this build, "false" otherwise.

  --os OS     Target distro, as CI names it (e.g. ubuntu-24.04,
              ubuntu-24.04-arm, centos-10). Anything but ubuntu-24.04 answers
              "false".
  --arch ARCH amd|arm -- picks which ci.sources file to probe.
EOF
}

need_value() {
  echo "Error: $1 requires a value" >&2
  print_help >&2
  exit 2
}

os=""
arch=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --os) [[ $# -ge 2 ]] || need_value "$1"; os="$2"; shift 2 ;;
    --arch) [[ $# -ge 2 ]] || need_value "$1"; arch="$2"; shift 2 ;;
    -h|--help) print_help; exit 0 ;;
    *)
      echo "Error: unknown argument '$1'" >&2
      print_help >&2
      exit 2
    ;;
  esac
done

if [[ -z "$os" || -z "$arch" ]]; then
  echo "Error: --os and --arch are required" >&2
  print_help >&2
  exit 2
fi

answer() {
  echo "$1"
  exit 0
}

if [[ -n "${MG_USE_CUSTOM_MIRROR:-}" ]]; then
  echo "use_custom_mirror: MG_USE_CUSTOM_MIRROR=$MG_USE_CUSTOM_MIRROR, skipping probe" >&2
  if [[ "$MG_USE_CUSTOM_MIRROR" == "true" ]]; then
    answer true
  fi
  answer false
fi

# CI appends -arm to the distro name for arm build containers.
if [[ "${os%-arm}" != "$SUPPORTED_OS" ]]; then
  echo "use_custom_mirror: os '$os' is not $SUPPORTED_OS" >&2
  answer false
fi

sources="$SCRIPT_DIR/$arch/ci.sources"
if [[ ! -f "$sources" ]]; then
  echo "Error: no sources file at $sources" >&2
  exit 2
fi

# Each stanza carries a single URI followed by its suites. Probe the Release
# file of the stanza's first suite -- that is the first thing apt fetches from
# that URI, so a mirror serving it serves the rest.
mapfile -t urls < <(awk '
  /^URIs:/               { uri = $2 }
  /^Suites:/ && uri != "" { print uri "/dists/" $2 "/Release"; uri = "" }
' "$sources")

if [[ ${#urls[@]} -eq 0 ]]; then
  echo "Error: no URIs/Suites pairs in $sources" >&2
  exit 2
fi

probe() {
  local url="$1" attempt
  for (( attempt = 1; attempt <= ATTEMPTS; attempt++ )); do
    if curl --head --location --fail --silent \
            --connect-timeout "$CONNECT_TIMEOUT" --max-time "$MAX_TIME" \
            --output /dev/null "$url"; then
      return 0
    fi
  done
  return 1
}

for url in "${urls[@]}"; do
  if ! probe "$url"; then
    echo "use_custom_mirror: $url unreachable, falling back to the default sources" >&2
    answer false
  fi
done

echo "use_custom_mirror: mirror reachable, using $sources" >&2
answer true
