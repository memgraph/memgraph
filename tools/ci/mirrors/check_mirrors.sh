#!/usr/bin/env bash
# Probe every mirror in pin_mirrors.sh against every repo it is expected to
# serve, for both arches, and report which combinations answer.
#
# Mirrors do drop distros -- ftp.fau.de answers 410 for centos-stream now, and
# a root that has quietly stopped carrying a repo would silently push every
# build onto the next entry in the list. Run this when adding a distro, when
# bumping to a new release, or when a mirror looks suspect:
#
#   tools/ci/mirrors/check_mirrors.sh                 # everything
#   tools/ci/mirrors/check_mirrors.sh --os fedora-44  # one distro
#
# Exits non-zero if any mirror fails to serve a repo it is listed for, so it
# can be run on a schedule. Nothing here touches the machine it runs on.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./pin_mirrors.sh
source "$SCRIPT_DIR/pin_mirrors.sh"
# pin_mirrors.sh sets -e for its own run; here a failing probe is a result to
# report, not a reason to stop.
set +e

CONNECT_TIMEOUT=5
MAX_TIME=20

# The distros CI builds on -- keep in step with the case list in
# package_smoke_image() (release/package/mgbuild.sh).
ALL_TARGETS=(
  "ubuntu 22.04 jammy"
  "ubuntu 24.04 noble"
  "ubuntu 26.04 resolute"
  "debian 12 bookworm"
  "debian 13 trixie"
  "centos 9 -"
  "centos 10 -"
  "rocky 10 -"
  "fedora 43 -"
  "fedora 44 -"
  "fedora 45 -"
)

only_os=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --os)
      [[ $# -ge 2 ]] || { echo "Error: --os requires a value" >&2; exit 2; }
      only_os="$2"; shift 2
    ;;
    -h|--help) sed -n '2,16p' "$0"; exit 0 ;;
    *) echo "Error: unknown argument '$1'" >&2; exit 2 ;;
  esac
done

failures=0
checked=0

probe() {
  curl --head --location --fail --silent \
       --connect-timeout "$CONNECT_TIMEOUT" --max-time "$MAX_TIME" \
       --output /dev/null "$1"
}

report() {
  local verdict="$1" root="$2" what="$3" detail="$4"
  checked=$(( checked + 1 ))
  if [[ "$verdict" == "OK" ]]; then
    printf '  ok    %-24s %s\n' "$what" "$root"
  else
    printf '  FAIL  %-24s %s\n      tried: %s\n' "$what" "$root" "$detail"
    failures=$(( failures + 1 ))
  fi
}

# One repo is satisfied by a root if any of the paths listed for it answers --
# Fedora legitimately has a release under development/ rather than releases/.
check_paths() {
  local root="$1" what="$2"
  shift 2
  local url tried=""
  for url in "$@"; do
    tried+="${tried:+ }$url"
    if probe "$url"; then
      report OK "$root" "$what" ""
      return
    fi
  done
  report FAIL "$root" "$what" "$tried"
}

check_apt_target() {
  local id="$1" ver="$2" codename="$3" arch="$4"
  # Read by the apt_roots/dnf_roots/dnf_repo_paths functions sourced above.
  # shellcheck disable=SC2034
  DISTRO_ID="$id"; DISTRO_VER="$ver"; DISTRO_MAJOR="${ver%%.*}"
  export MG_MIRROR_ARCH="$arch"

  echo "== $id $ver ($codename, $arch)"
  local kind root suite suites
  for kind in archive security; do
    if [[ "$id" == "ubuntu" && "$kind" == "security" ]]; then
      continue  # served from the archive roots, already covered
    fi
    if [[ "$kind" == "security" ]]; then
      suites="$codename-security"
    elif [[ "$id" == "ubuntu" ]]; then
      suites="$codename $codename-updates $codename-backports $codename-security"
    else
      suites="$codename $codename-updates"
    fi
    while read -r root; do
      [[ -n "$root" ]] || continue
      for suite in $suites; do
        check_paths "$root" "$suite" "$root/dists/$suite/Release"
      done
    done < <(apt_roots "$kind")
  done
}

check_dnf_target() {
  local id="$1" ver="$2" arch="$3"
  # Read by the apt_roots/dnf_roots/dnf_repo_paths functions sourced above.
  # shellcheck disable=SC2034
  DISTRO_ID="$id"; DISTRO_VER="$ver"; DISTRO_MAJOR="${ver%%.*}"
  export MG_MIRROR_ARCH="$arch"

  echo "== $id $ver ($arch)"
  local root line repo_id paths path
  while read -r root; do
    [[ -n "$root" ]] || continue
    while read -r line; do
      [[ -n "$line" ]] || continue
      repo_id="${line%% *}"
      paths="${line#* }"
      local -a urls=()
      for path in $paths; do
        urls+=("$root/${path%/}/repodata/repomd.xml")
      done
      check_paths "$root" "$repo_id" "${urls[@]}"
    done < <(dnf_repo_paths)
  done < <(dnf_roots)
}

for target in "${ALL_TARGETS[@]}"; do
  read -r id ver codename <<<"$target"
  if [[ -n "$only_os" && "$only_os" != "$id-$ver" && "$only_os" != "$id" ]]; then
    continue
  fi
  for arch in amd arm; do
    case "$id" in
      ubuntu|debian) check_apt_target "$id" "$ver" "$codename" "$arch" ;;
      *)             check_dnf_target "$id" "$ver" "$arch" ;;
    esac
  done
done

echo
if [[ "$checked" -eq 0 ]]; then
  echo "No targets matched${only_os:+ --os $only_os}." >&2
  exit 2
fi
echo "$checked checks, $failures failed"
[[ "$failures" -eq 0 ]]
