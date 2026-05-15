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
APT_DEBS=()
for deb in "$DEBS_DIR"/*.deb; do
  case "$(basename "$deb")" in
    memgraph_*.deb|memgraph-mage_*.deb) continue ;;
    *) APT_DEBS+=("$deb") ;;
  esac
done

for attempt in 1 2 3; do
  if dpkg -i "${APT_DEBS[@]}"; then
    break
  fi
  echo "==> dpkg pass $attempt left packages half-installed, running --configure -a"
  dpkg --configure -a
done
# Final verification: any half-configured package here is a real bug, not an
# ordering glitch, so let the script die.
dpkg --configure -a

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
  groupadd -g 103 memgraph
fi
if ! id memgraph >/dev/null 2>&1; then
  useradd -u 101 -g memgraph -m -d /home/memgraph -s /bin/bash memgraph
fi

# ---------------------------------------------------------------------------
# 3. Install all Python wheels as the memgraph user, offline.
# ---------------------------------------------------------------------------
echo "==> Installing Python wheels into /home/memgraph/.local"

# pip.conf locks pip into our local wheel directory for every user on the box
# for the duration of step 3 + step 4 (the memgraph-mage postinst runs pip
# install too — see mage/install_python_requirements.sh --deb-package).
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

# Install every wheel in one shot. pip resolves the dep graph using the wheels
# in $WHEELS_DIR and never reaches out to PyPI because of pip.conf above.
chown -R memgraph:memgraph "$WHEELS_DIR"
su - memgraph -c "python3 -m pip install --no-cache-dir $WHEELS_DIR/*.whl"

# ---------------------------------------------------------------------------
# 4. Install memgraph and memgraph-mage debs.
# ---------------------------------------------------------------------------
echo "==> Installing memgraph + memgraph-mage"

MEMGRAPH_DEB=$(ls "$DEBS_DIR"/memgraph_*.deb 2>/dev/null | head -1 || true)
MAGE_DEB=$(ls "$DEBS_DIR"/memgraph-mage_*.deb 2>/dev/null | head -1 || true)

if [[ -z "$MEMGRAPH_DEB" ]]; then
  echo "Error: no memgraph_*.deb found in $DEBS_DIR" >&2
  exit 1
fi
if [[ -z "$MAGE_DEB" ]]; then
  echo "Error: no memgraph-mage_*.deb found in $DEBS_DIR" >&2
  exit 1
fi

# All Depends: from these debs (openssl, python3, libcurl4, etc.) were
# already installed and configured in step 1. memgraph-mage Depends: memgraph
# so install order matters; loop with --configure -a between passes in case
# of any Pre-Depends/ordering quirks (same pattern as step 1).
for attempt in 1 2 3; do
  if dpkg -i "$MEMGRAPH_DEB" "$MAGE_DEB"; then
    break
  fi
  dpkg --configure -a
done
dpkg --configure -a

echo
echo "Memgraph + MAGE installed successfully."
echo "Start with: systemctl start memgraph"
