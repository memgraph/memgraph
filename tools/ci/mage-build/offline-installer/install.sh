#!/bin/bash
set -euo pipefail
# Installs Memgraph + MAGE on a vanilla Ubuntu 24.04 host with no network
# access. Run as root after the self-extracting .run file has expanded the
# bundle into BUNDLE_DIR.
#
# Bundle layout:
#   debs/                — every .deb needed: apt deps, dpkg-dev, memgraph, memgraph-mage
#   wheels/              — every .whl needed by mage + auth modules
#   install.sh           — this script

BUNDLE_DIR=${BUNDLE_DIR:-"$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"}
DEBS_DIR="$BUNDLE_DIR/debs"
WHEELS_DIR="$BUNDLE_DIR/wheels"
REQS_DIR="$BUNDLE_DIR/requirements"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Error: install.sh must run as root (try: sudo ./memgraph-mage-offline.run)" >&2
  exit 1
fi

. /etc/os-release 2>/dev/null || true
if [[ "${ID:-}" != "ubuntu" || "${VERSION_ID:-}" != "24.04" ]]; then
  echo "Warning: this installer targets Ubuntu 24.04, found ${ID:-unknown} ${VERSION_ID:-unknown}." >&2
  echo "         Continuing, but results are unsupported." >&2
fi

export DEBIAN_FRONTEND=noninteractive

# Verbose output from dpkg goes to this log rather than the terminal. dpkg's
# multi-pass install (see step 1) legitimately prints "dpkg: error processing
# ..." on its early passes and recovers on the next one, which reads as a
# failed install to anyone watching. Keep the noise here and surface it only
# when a phase genuinely fails.
INSTALL_LOG="${INSTALL_LOG:-/var/log/memgraph-offline-install.log}"
: > "$INSTALL_LOG" 2>/dev/null || INSTALL_LOG=/tmp/memgraph-offline-install.log
: > "$INSTALL_LOG"

die() {
  echo "Error: $1" >&2
  echo "--- last 50 lines of $INSTALL_LOG ---" >&2
  tail -n 50 "$INSTALL_LOG" >&2 || true
  echo "--- full log: $INSTALL_LOG ---" >&2
  exit 1
}

# ---------------------------------------------------------------------------
# 1. Install apt dependencies directly from the bundled .deb files.
# ---------------------------------------------------------------------------
echo "==> Installing apt dependencies from bundled .deb files"

# Pre-accept the msodbcsql18 EULA before its postinst runs.
echo "msodbcsql18 msodbcsql/ACCEPT_EULA boolean true" | debconf-set-selections
export ACCEPT_EULA=Y

# Install every apt dep deb in a single dpkg call so dpkg can resolve all
# in-batch dependencies. We exclude the memgraph + memgraph-mage debs here —
# they get installed in step 4 once the python wheels are in place. Multiple
# passes with `dpkg --configure -a` between them are needed because some
# packages (notably python3) carry Pre-Depends that require their prereq to
# be *configured*, not merely unpacked, before unpack can proceed. The
# configure-then-retry loop converges within two or three rounds.
# memgraph.deb arrives renamed by the workflow (mv memgraph_x.y.z_amd64.deb
# -> memgraph.deb in reusable_package_mage.yaml so the Dockerfile build
# context can find it at a stable path). memgraph-mage keeps its versioned
# filename. Exclude both forms here — they install in step 4.
# nullglob makes the loop body skip entirely when the glob doesn't match
# (instead of bash's default of iterating once with the literal pattern,
# which would feed dpkg a bogus path).
shopt -s nullglob
APT_DEBS=()
for deb in "$DEBS_DIR"/*.deb; do
  case "$(basename "$deb")" in
    memgraph.deb|memgraph_*.deb|memgraph-mage.deb|memgraph-mage_*.deb) continue ;;
    *) APT_DEBS+=("$deb") ;;
  esac
done
shopt -u nullglob

if [[ ${#APT_DEBS[@]} -eq 0 ]]; then
  echo "Error: no apt-dep .debs found in $DEBS_DIR — bundle is incomplete." >&2
  exit 1
fi

echo "    installing ${#APT_DEBS[@]} packages, this takes a minute (log: $INSTALL_LOG)"
for attempt in 1 2 3; do
  if dpkg -i "${APT_DEBS[@]}" >>"$INSTALL_LOG" 2>&1; then
    break
  fi
  echo "    resolving dependency order (pass $attempt) ..."
  # `|| true` is critical here: the configure pass *will* fail until enough
  # packages are unpacked (e.g. python3-pip can't configure until python3 is
  # there). We need to let the loop iterate so the next dpkg -i can unpack
  # the packages whose Pre-Depends are now satisfied. set -euo pipefail would
  # otherwise kill the script after this line. The dpkg errors these passes
  # emit are expected and stay in $INSTALL_LOG.
  dpkg --configure -a >>"$INSTALL_LOG" 2>&1 || true
done
# Final verification: any half-configured package here is a real bug, not an
# ordering glitch, so fail loudly and show what dpkg said.
dpkg --configure -a >>"$INSTALL_LOG" 2>&1 \
  || die "apt dependencies could not be configured"

# Match install_runtime_requirements.sh: provide the libaio.so.1 symlink some
# of the python modules link against.
if [[ ! -e /usr/lib/x86_64-linux-gnu/libaio.so.1 \
   && -e /usr/lib/x86_64-linux-gnu/libaio.so.1t64 ]]; then
  ln -sv /usr/lib/x86_64-linux-gnu/libaio.so.1t64 /usr/lib/x86_64-linux-gnu/libaio.so.1
fi

# ---------------------------------------------------------------------------
# 2. Create the memgraph user (mirrors Dockerfile.release).
# ---------------------------------------------------------------------------
echo "==> Creating memgraph user"
if ! getent group memgraph >/dev/null; then
  groupadd -g 103 memgraph >>"$INSTALL_LOG" 2>&1 || die "could not create the memgraph group"
fi
if ! id memgraph >/dev/null 2>&1; then
  # Output to the log: the fixed uid/gid (matching Dockerfile.release) is below
  # useradd's UID_MIN, so it warns every time. That is deliberate, not a fault.
  useradd -u 101 -g memgraph -m -d /home/memgraph -s /bin/bash memgraph \
    >>"$INSTALL_LOG" 2>&1 || die "could not create the memgraph user"
fi

# ---------------------------------------------------------------------------
# 3. Install all Python wheels as the memgraph user, offline.
# ---------------------------------------------------------------------------
echo "==> Installing Python wheels into /home/memgraph/.local"

# pip.conf locks pip into our local wheel directory for every user on the box
# for the duration of step 3 + step 4 (the memgraph-mage postinst runs pip
# install too — see release/package/mage/install_python_requirements.sh --deb-package).
PIP_CONF_BACKUP="/etc/pip.conf.memgraph-offline.bak"
[[ -f /etc/pip.conf ]] && mv /etc/pip.conf "$PIP_CONF_BACKUP" || true
cat > /etc/pip.conf <<EOF
[global]
no-index = true
find-links = $WHEELS_DIR
break-system-packages = true
EOF

restore_pip_conf() {
  rm -f /etc/pip.conf
  [[ -f "$PIP_CONF_BACKUP" ]] && mv "$PIP_CONF_BACKUP" /etc/pip.conf || true
}

trap restore_pip_conf EXIT

chown -R memgraph:memgraph "$WHEELS_DIR" "$REQS_DIR"

# Install via the bundled requirements files (the same ones we used for
# `pip download` at build time). pip resolves to one version per package
# using the pins in these files; passing wheels directly via `pip install
# *.whl` instead would fail because the wheel cache contains multiple
# versions of some packages (e.g. cryptography 46 from auth-module pin +
# cryptography 48 from oracledb's looser >=3.2.1 constraint).
# --no-warn-script-location: pip otherwise emits a WARNING for each of the ~20
# console scripts it lays down in /home/memgraph/.local/bin, none of which are
# meant to be run from a shell — memgraph imports these packages as libraries.
# It has to be a command-line flag: as a pip.conf key it silently does nothing,
# because pip assigns config values straight to the option's dest and this one
# is a store_false (`no-warn-script-location = true` would *enable* warnings).
su - memgraph -c "python3 -m pip install --no-cache-dir --no-warn-script-location \
  -r $REQS_DIR/mage-requirements.txt \
  -r $REQS_DIR/auth-module-requirements.txt"

# Now the PyG / DGL extras that don't appear in any requirements file
# (install_python_requirements.sh installs them by S3 URL). Resolved by
# name from the wheel cache via pip.conf's find-links.
su - memgraph -c "python3 -m pip install --no-cache-dir --no-warn-script-location \
  torch-cluster torch-geometric torch-scatter torch-sparse torch-spline-conv dgl"

# ---------------------------------------------------------------------------
# 4. Install memgraph and memgraph-mage debs.
# ---------------------------------------------------------------------------
echo "==> Installing memgraph + memgraph-mage"

# Accept both filename forms (see step-1 comment).
MEMGRAPH_DEB=""
for candidate in "$DEBS_DIR"/memgraph.deb "$DEBS_DIR"/memgraph_*.deb; do
  if [[ -f "$candidate" ]]; then MEMGRAPH_DEB="$candidate"; break; fi
done
MAGE_DEB=""
for candidate in "$DEBS_DIR"/memgraph-mage.deb "$DEBS_DIR"/memgraph-mage_*.deb; do
  if [[ -f "$candidate" ]]; then MAGE_DEB="$candidate"; break; fi
done

if [[ -z "$MEMGRAPH_DEB" ]]; then
  echo "Error: no memgraph .deb found in $DEBS_DIR (expected memgraph.deb or memgraph_*.deb)" >&2
  exit 1
fi
if [[ -z "$MAGE_DEB" ]]; then
  echo "Error: no memgraph-mage .deb found in $DEBS_DIR (expected memgraph-mage.deb or memgraph-mage_*.deb)" >&2
  exit 1
fi

# MG_SKIP_PYTHON_DEPS=1 tells both postinsts (memgraph's and MAGE's) to skip
# their pip phase outright. Step 3 already installed every python dep from the
# bundled wheels, so that phase has nothing left to do — but it installs some
# packages by explicit https URL, which pip attempts regardless of what is
# already present. Offline that means ~8 DNS retries per package and a pair of
# alarming "ERROR: Could not install packages due to an OSError" blocks before
# it gives up. Skipping is both quieter and considerably faster.
export MG_SKIP_PYTHON_DEPS=1
# Belt and braces: should a future postinst grow another network-dependent
# step, MEMGRAPH_OFFLINE_INSTALL=1 tells it to treat the failure as a warning
# instead of a hard error. Unset for normal online apt/dpkg installs, so a
# genuine pip failure there still fails loudly rather than silently leaving
# MAGE broken.
export MEMGRAPH_OFFLINE_INSTALL=1

# All Depends: from these debs (openssl, python3, libcurl4, etc.) were
# already installed and configured in step 1. memgraph-mage Depends: memgraph
# so install order matters; loop with --configure -a between passes in case
# of any Pre-Depends/ordering quirks (same pattern as step 1).
for attempt in 1 2 3; do
  if dpkg -i "$MEMGRAPH_DEB" "$MAGE_DEB" >>"$INSTALL_LOG" 2>&1; then
    break
  fi
  echo "    resolving dependency order (pass $attempt) ..."
  dpkg --configure -a >>"$INSTALL_LOG" 2>&1 || true
done
dpkg --configure -a >>"$INSTALL_LOG" 2>&1 \
  || die "memgraph + memgraph-mage could not be configured"

echo
echo "Memgraph + MAGE installed successfully."
echo "Start with: systemctl start memgraph"
echo "Installation log: $INSTALL_LOG"
