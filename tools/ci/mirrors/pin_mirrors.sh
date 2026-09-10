#!/usr/bin/env bash
# Point a container's distro package repos at a vetted, ordered mirror list.
#
# Every distro we build on resolves its repos through a metalink/mirrorlist
# redirector that hands out whichever mirror it likes at that moment. In CI
# that regularly lands on a mirror that is mid-sync -- dnf fails the repomd
# checksum, apt fails a hash sum mismatch -- and the build dies for reasons
# that have nothing to do with memgraph. This replaces that lottery with an
# explicit, ordered list of mirrors, keeping the distro's own default last so
# the worst case is the behaviour we had before.
#
# The two package managers need that done differently. dnf takes a
# comma-separated baseurl and fails over through it itself, so the list goes
# straight into the repo config. apt has no equivalent: its mirror+file method
# disregards the order and spends minutes retrying mirrors it has already
# ignored, so here we pick one mirror, prove it with the `apt-get update` the
# caller needs anyway, and only move to the next candidate if that fails.
#
# Runs INSIDE the image being built: the distro and arch come from
# /etc/os-release and uname, so callers never have to say which one it is,
# and an unrecognised distro is a no-op rather than an error.
#
#   apply    rewrite the repo config (default). Verifies the result with a
#            metadata refresh and, if that fails, restores the original config
#            and still exits 0 -- pinning must never itself break a build.
#   restore  put the original repo config back. Shipping images call this
#            after installing so the delivered image carries stock sources.
#   print    print the mirror roots for the detected distro and exit.
#
# Maintaining the lists: every root must serve the layouts in apt_roots() /
# dnf_repo_paths() for both arches we build (x86_64 and aarch64), and the
# distro's own default belongs last. tools/ci/mirrors/check_mirrors.sh probes
# every URL these tables can produce -- run it when adding a distro or when a
# mirror starts 404ing (mirrors do drop distros: ftp.fau.de now answers 410
# for centos-stream, which is why it is not in that list).

set -euo pipefail

BACKUP_SUFFIX=".mg-orig"
APT_CONF="/etc/apt/apt.conf.d/99-mg-ci-mirrors"
DNF_CONF="/etc/dnf/dnf.conf"

log() { echo "pin_mirrors: $*" >&2; }

# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

DISTRO_ID=""
DISTRO_VER=""
DISTRO_MAJOR=""

detect_distro() {
  if [[ ! -r /etc/os-release ]]; then
    log "no /etc/os-release, nothing to pin"
    exit 0
  fi
  # shellcheck disable=SC1091
  . /etc/os-release
  DISTRO_ID="${ID:-}"
  DISTRO_VER="${VERSION_ID:-}"
  DISTRO_MAJOR="${DISTRO_VER%%.*}"
}

# dnf's $basearch / apt's dpkg architecture, from the running container -- under
# qemu emulation uname already reports the target arch, which is what we want.
# MG_MIRROR_ARCH (amd|arm) overrides it, which is how check_mirrors.sh probes
# the arm lists from an amd host.
mirror_arch() {
  case "${MG_MIRROR_ARCH:-$(uname -m)}" in
    arm|aarch64|arm64) echo arm ;;
    *)                 echo amd ;;
  esac
}

rpm_arch() {
  [[ "$(mirror_arch)" == "arm" ]] && echo aarch64 || echo x86_64
}

dpkg_arch() {
  [[ "$(mirror_arch)" == "arm" ]] && echo arm64 || echo amd64
}

# ---------------------------------------------------------------------------
# Mirror tables
# ---------------------------------------------------------------------------

# Roots for an apt source kind: "archive" or "security", most preferred first
# and the distro's own host last. Ubuntu's mirrors carry <suite>-security under
# the same root, so both kinds share one list there; Debian keeps security on a
# separate archive.
#
# These are http, not https, and deliberately so: the ubuntu/debian base images
# ship no CA store, so apt cannot verify any TLS certificate in them ("No
# system certificates available") and every https source fails the handshake.
# That is why the distros' own sources are http too -- apt authenticates the
# repo by its GPG signature, which is what actually protects the packages.
apt_roots() {
  local kind="$1"
  case "$DISTRO_ID" in
    ubuntu)
      if [[ "$(dpkg_arch)" == "arm64" ]]; then
        # arm64 is not in the main Ubuntu archive; it lives in ubuntu-ports,
        # which far fewer mirrors carry.
        printf '%s\n' \
          "http://ftp.fau.de/ubuntu-ports" \
          "http://mirror.dogado.de/ubuntu-ports" \
          "http://ftp.tu-chemnitz.de/pub/linux/ubuntu-ports" \
          "http://ports.ubuntu.com/ubuntu-ports"
      else
        printf '%s\n' \
          "http://ftp.halifax.rwth-aachen.de/ubuntu" \
          "http://ftp.fau.de/ubuntu" \
          "http://mirror.init7.net/ubuntu" \
          "http://archive.ubuntu.com/ubuntu"
      fi
    ;;
    debian)
      if [[ "$kind" == "security" ]]; then
        printf '%s\n' \
          "http://mirror.init7.net/debian-security" \
          "http://mirror.dogado.de/debian-security" \
          "http://security.debian.org/debian-security"
      else
        printf '%s\n' \
          "http://ftp.halifax.rwth-aachen.de/debian" \
          "http://ftp.fau.de/debian" \
          "http://mirror.init7.net/debian" \
          "http://deb.debian.org/debian"
      fi
    ;;
  esac
}

dnf_roots() {
  case "$DISTRO_ID" in
    centos)
      printf '%s\n' \
        "https://ftp.plusline.net/centos-stream" \
        "https://ftp.gwdg.de/pub/linux/centos-stream" \
        "https://centos.anexia.at/centos-stream" \
        "https://mirror.stream.centos.org"
    ;;
    rocky)
      printf '%s\n' \
        "https://ftp.gwdg.de/pub/linux/rocky" \
        "https://mirror.23m.com/rocky" \
        "https://mirror1.hs-esslingen.de/pub/Mirrors/rocky" \
        "https://mirror.dogado.de/rockylinux" \
        "https://dl.rockylinux.org/pub/rocky"
    ;;
    fedora)
      printf '%s\n' \
        "https://ftp.halifax.rwth-aachen.de/fedora/linux" \
        "https://ftp.gwdg.de/pub/linux/fedora/linux" \
        "https://ftp.fau.de/fedora/linux" \
        "https://dl.fedoraproject.org/pub/fedora/linux"
    ;;
  esac
}

# "<repo-id> <path>[ <path>...]" per line: the path(s) under each root that
# serve that repo. A repo with several paths gets them all, tried in order --
# Fedora needs that because a release is under development/ until it ships and
# releases/ afterwards, and we should not have to track which it is today.
dnf_repo_paths() {
  local arch
  arch="$(rpm_arch)"
  case "$DISTRO_ID" in
    centos)
      local stream="${DISTRO_MAJOR}-stream"
      printf '%s\n' \
        "baseos $stream/BaseOS/$arch/os/" \
        "appstream $stream/AppStream/$arch/os/" \
        "crb $stream/CRB/$arch/os/" \
        "extras-common SIGs/$stream/extras/$arch/extras-common/"
    ;;
    rocky)
      printf '%s\n' \
        "baseos $DISTRO_MAJOR/BaseOS/$arch/os/" \
        "appstream $DISTRO_MAJOR/AppStream/$arch/os/" \
        "crb $DISTRO_MAJOR/CRB/$arch/os/" \
        "extras $DISTRO_MAJOR/extras/$arch/os/"
    ;;
    fedora)
      printf '%s\n' \
        "fedora releases/$DISTRO_VER/Everything/$arch/os/ development/$DISTRO_VER/Everything/$arch/os/" \
        "updates updates/$DISTRO_VER/Everything/$arch/"
    ;;
  esac
}

# ---------------------------------------------------------------------------
# Backup / restore
# ---------------------------------------------------------------------------

backup_file() {
  local f="$1"
  # First apply wins, so a second apply doesn't overwrite the pristine copy
  # with an already-pinned one.
  [[ -e "$f$BACKUP_SUFFIX" ]] || cp -a "$f" "$f$BACKUP_SUFFIX"
}

restore_globs() {
  local backup f quiet=false
  if [[ "${1:-}" == "--quiet" ]]; then
    quiet=true
    shift
  fi
  shopt -s nullglob
  for backup in "$@"; do
    # nullglob drops the patterns that matched nothing, but the fixed paths in
    # the argument list survive whether or not they exist -- and most of them
    # won't, since a given image is either apt- or dnf-based.
    [[ -e "$backup" ]] || continue
    f="${backup%"$BACKUP_SUFFIX"}"
    mv -f "$backup" "$f"
    [[ "$quiet" == "true" ]] || log "restored $f"
  done
  shopt -u nullglob
}

# Just the apt sources, keeping the backups' contents available again: the
# probe loop below rewrites from pristine sources on every attempt, because a
# second rewrite of an already-pinned file would find no distro host to match.
restore_apt_sources() {
  restore_globs --quiet \
    /etc/apt/sources.list"$BACKUP_SUFFIX" \
    /etc/apt/sources.list.d/*"$BACKUP_SUFFIX"
}

restore_all() {
  restore_globs \
    /etc/apt/sources.list"$BACKUP_SUFFIX" \
    /etc/apt/sources.list.d/*"$BACKUP_SUFFIX" \
    /etc/yum.repos.d/*"$BACKUP_SUFFIX" \
    "$DNF_CONF$BACKUP_SUFFIX"
  rm -f "$APT_CONF"
}

# ---------------------------------------------------------------------------
# apt
# ---------------------------------------------------------------------------

# Rewrite the archive URIs in one sources file to point at the given roots.
# Only the distro's own hosts are touched: a third-party repo (nodesource,
# say) is left exactly as it was, and everything but the URI -- suites,
# components, signing keys -- stays as the distro shipped it.
rewrite_apt_file() {
  local file="$1" archive="$2" security="$3" tmp
  tmp="$(mktemp)"
  awk -v distro="$DISTRO_ID" -v archive="$archive" -v security="$security" '
    function is_distro_host(uri) {
      if (distro == "ubuntu") return uri ~ /^[a-z+.-]+:\/\/([A-Za-z0-9._-]+\.)?ubuntu\.com(\/|$)/
      if (distro == "debian") return uri ~ /^[a-z+.-]+:\/\/([A-Za-z0-9._-]+\.)?debian\.org(\/|$)/
      return 0
    }
    # Ubuntu serves <suite>-security from its archive roots, so only Debian
    # sends anything to a separate security root.
    function replacement(uri) {
      if (security != "" && (uri ~ /debian-security/ || uri ~ /:\/\/security\.debian\.org/)) return security
      return archive
    }
    # deb822 (.sources): a URIs: line may carry several URIs.
    /^[[:space:]]*URIs:/ {
      out = ""
      for (i = 2; i <= NF; i++) {
        tok = is_distro_host($i) ? replacement($i) : $i
        if (out != tok) out = (out == "" ? tok : out " " tok)
      }
      print "URIs: " out
      next
    }
    # one-line (.list): deb [options] URI suite components...
    /^[[:space:]]*deb(-src)?[[:space:]]/ {
      for (i = 1; i <= NF; i++)
        if ($i ~ /:\/\// && is_distro_host($i)) $i = replacement($i)
      print
      next
    }
    { print }
  ' "$file" > "$tmp"
  cat "$tmp" > "$file"
  rm -f "$tmp"
}

apt_sources_files() {
  shopt -s nullglob
  local f
  for f in /etc/apt/sources.list /etc/apt/sources.list.d/*.list /etc/apt/sources.list.d/*.sources; do
    if [[ -f "$f" ]]; then
      printf '%s\n' "$f"
    fi
  done
  shopt -u nullglob
}

# apt has no usable per-source failover -- its mirror+file method ignores the
# preference order and burns minutes retrying -- so do the failover here
# instead: pin one mirror, prove it with the update we have to run anyway, and
# move to the next one only if that fails. The last candidate is the distro's
# own host, so the final attempt is exactly the unpinned behaviour.
apt_apply() {
  local -a archive_roots security_roots sources
  mapfile -t archive_roots < <(apt_roots archive)
  mapfile -t security_roots < <(apt_roots security)
  mapfile -t sources < <(apt_sources_files)

  if [[ ${#archive_roots[@]} -eq 0 ]]; then
    log "no mirror list for $DISTRO_ID $DISTRO_VER"
    return 1
  fi
  if [[ ${#sources[@]} -eq 0 ]]; then
    log "no apt sources files found"
    return 1
  fi

  cat > "$APT_CONF" <<'EOF'
// Generated by tools/ci/mirrors/pin_mirrors.sh. One retry rides out a blip;
// past that we would rather fall through to the next mirror than sit on a
// stalled one, so the timeouts are deliberately short.
Acquire::Retries "1";
Acquire::http::Timeout "15";
Acquire::https::Timeout "15";
EOF
  chmod 0644 "$APT_CONF"

  local f i archive security
  for (( i = 0; i < ${#archive_roots[@]}; i++ )); do
    archive="${archive_roots[i]}"
    security=""
    if [[ ${#security_roots[@]} -gt 0 ]]; then
      # Zip the two lists; the shorter one holds on its last entry.
      if (( i < ${#security_roots[@]} )); then
        security="${security_roots[i]}"
      else
        security="${security_roots[${#security_roots[@]} - 1]}"
      fi
      [[ "$security" == "$archive" ]] && security=""
    fi

    restore_apt_sources
    for f in "${sources[@]}"; do
      backup_file "$f"
      rewrite_apt_file "$f" "$archive" "$security"
    done

    log "trying $archive for $DISTRO_ID $DISTRO_VER ($(dpkg_arch))"
    # Error-Mode=any is what makes this a real test: by default apt-get update
    # reports a source it could not fetch as a warning and still exits 0, so
    # without this a mirror that served nothing would look like a success.
    if apt-get update -o APT::Update::Error-Mode=any; then
      log "pinned apt to $archive${security:+ (security: $security)}"
      return 0
    fi
    log "$archive did not serve a usable index, moving on"
  done
  return 1
}

# ---------------------------------------------------------------------------
# dnf
# ---------------------------------------------------------------------------

PINNED_REPO_IDS=""

# Comment out the metalink/mirrorlist/baseurl of the named sections and give
# them an explicit ordered baseurl list instead. Sections we have no path for
# keep their original config.
rewrite_repo_file() {
  local file="$1" spec="$2" tmp
  tmp="$(mktemp)"
  awk -v spec="$spec" '
    BEGIN {
      n = split(spec, entries, "\n")
      for (i = 1; i <= n; i++) {
        if (entries[i] == "") continue
        eq = index(entries[i], "=")
        urls[substr(entries[i], 1, eq - 1)] = substr(entries[i], eq + 1)
      }
    }
    /^[[:space:]]*\[/ {
      section = $0
      sub(/^[[:space:]]*\[/, "", section)
      sub(/\].*$/, "", section)
      pinned = (section in urls)
      print
      if (pinned) print "baseurl=" urls[section]
      next
    }
    pinned && /^[[:space:]]*(metalink|mirrorlist|baseurl)[[:space:]]*=/ {
      print "#" $0 "  # pin_mirrors.sh"
      next
    }
    { print }
  ' "$file" > "$tmp"
  cat "$tmp" > "$file"
  rm -f "$tmp"
}

dnf_apply() {
  local -a roots
  mapfile -t roots < <(dnf_roots)
  if [[ ${#roots[@]} -eq 0 ]]; then
    log "no mirror list for $DISTRO_ID $DISTRO_VER, leaving dnf alone"
    return 0
  fi

  # Build "<repo-id>=<url>,<url>,..." lines. Mirror order is the outer loop, so
  # our preferred mirror gets tried for every path it might serve the repo from
  # before we move on to the next mirror.
  local spec="" line repo_id paths root path urls
  while read -r line; do
    [[ -n "$line" ]] || continue
    repo_id="${line%% *}"
    paths="${line#* }"
    # Only claim repos this image actually defines -- the table covers a
    # family, and a given base image may not ship every one of its repos.
    # Naming an absent repo would also make the verify below fail on
    # --enablerepo and throw away good pinning.
    if ! grep -qs "^[[:space:]]*\[$repo_id\]" /etc/yum.repos.d/*.repo; then
      continue
    fi
    urls=""
    for root in "${roots[@]}"; do
      for path in $paths; do
        urls+="${urls:+,}$root/$path"
      done
    done
    spec+="${repo_id}=${urls}"$'\n'
    PINNED_REPO_IDS+="${PINNED_REPO_IDS:+,}$repo_id"
  done < <(dnf_repo_paths)

  if [[ -z "$spec" ]]; then
    log "no repo paths for $DISTRO_ID $DISTRO_VER, leaving dnf alone"
    return 0
  fi

  local f found=0
  shopt -s nullglob
  for f in /etc/yum.repos.d/*.repo; do
    [[ -f "$f" ]] || continue
    backup_file "$f"
    rewrite_repo_file "$f" "$spec"
    found=1
  done
  shopt -u nullglob
  if [[ "$found" -eq 0 ]]; then
    log "no .repo files found"
    return 0
  fi

  if [[ -f "$DNF_CONF" ]]; then
    backup_file "$DNF_CONF"
    # fastestmirror reorders our deliberate ordering; the rest just makes dnf
    # persist against a slow or stalling mirror instead of giving up.
    {
      echo "retries=5"
      echo "timeout=60"
      echo "minrate=1k"
      echo "fastestmirror=0"
    } >> "$DNF_CONF"
  fi
  log "pinned dnf repos [$PINNED_REPO_IDS] to ${#roots[@]} mirrors for $DISTRO_ID $DISTRO_VER ($(rpm_arch))"
}

# Refresh only what we repointed: an unrelated repo failing (Fedora's openh264
# repo lives on its own host) must not make us throw away good pinning.
dnf_verify() {
  [[ -n "$PINNED_REPO_IDS" ]] || return 0
  dnf -q --disablerepo='*' --enablerepo="$PINNED_REPO_IDS" makecache --refresh
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

package_family() {
  case "$DISTRO_ID" in
    ubuntu|debian)        echo apt ;;
    centos|rocky|fedora)  echo dnf ;;
    *)                    echo none ;;
  esac
}

do_apply() {
  local family
  family="$(package_family)"
  case "$family" in
    apt)
      if ! apt_apply; then
        log "no candidate mirror served a usable index, reverting to the distro default"
        restore_all
        # Leave the caller's install to be the thing that reports a genuinely
        # unreachable archive; pinning must never be the failure.
        apt-get update || true
      fi
    ;;
    dnf)
      dnf_apply
      if ! dnf_verify; then
        log "dnf makecache failed against the pinned mirrors, reverting to the distro default"
        restore_all
      fi
    ;;
    *)
      log "$DISTRO_ID is not a distro we pin mirrors for, nothing to do"
    ;;
  esac
}

do_print() {
  case "$(package_family)" in
    apt)
      echo "# $DISTRO_ID $DISTRO_VER ($(dpkg_arch)) archive"
      apt_roots archive
      if [[ "$DISTRO_ID" == "debian" ]]; then
        echo "# $DISTRO_ID $DISTRO_VER security"
        apt_roots security
      fi
    ;;
    dnf)
      echo "# $DISTRO_ID $DISTRO_VER ($(rpm_arch)) roots"
      dnf_roots
      echo "# repo paths"
      dnf_repo_paths
    ;;
    *) echo "# $DISTRO_ID: not pinned" ;;
  esac
}

main() {
  local action="${1:-apply}"
  detect_distro
  case "$action" in
    apply)   do_apply ;;
    restore) restore_all ;;
    print)   do_print ;;
    -h|--help)
      sed -n '2,30p' "$0"
    ;;
    *)
      echo "Error: unknown action '$action' (expected apply, restore or print)" >&2
      exit 2
    ;;
  esac
}

# Sourceable so check_mirrors.sh can walk the tables above without applying
# anything.
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
