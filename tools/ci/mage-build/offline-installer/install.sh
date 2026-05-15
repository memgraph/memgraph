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
# 1. Install apt dependencies from a temporary local apt repository.
# ---------------------------------------------------------------------------
echo "==> Installing apt dependencies from bundled .deb files"

# dpkg-scanpackages lives in dpkg-dev; we bundle dpkg-dev itself. Install it
# with dpkg first so we can scan the rest into a local Packages index.
# (Two passes — dpkg doesn't topo-sort across a single -i call.)
dpkg -i "$DEBS_DIR"/dpkg-dev*.deb "$DEBS_DIR"/libdpkg-perl*.deb 2>/dev/null || true
dpkg -i "$DEBS_DIR"/dpkg-dev*.deb "$DEBS_DIR"/libdpkg-perl*.deb

# Build the local repo index inside the bundle directory.
(
  cd "$DEBS_DIR"
  dpkg-scanpackages --multiversion . /dev/null > Packages
  gzip -9kf Packages
)

# Point apt at the local repo and only the local repo for the duration of the
# install — this is how we guarantee zero network use. Save existing sources
# so we can restore them when we're done.
SOURCES_BACKUP="/etc/apt/sources.list.memgraph-offline.bak"
SOURCES_D_BACKUP="/etc/apt/sources.list.d.memgraph-offline.bak"
[[ -f /etc/apt/sources.list ]] && mv /etc/apt/sources.list "$SOURCES_BACKUP" || true
[[ -d /etc/apt/sources.list.d ]] && mv /etc/apt/sources.list.d "$SOURCES_D_BACKUP" || true
mkdir -p /etc/apt/sources.list.d
echo "deb [trusted=yes] file:$DEBS_DIR ./" > /etc/apt/sources.list.d/memgraph-offline.list

restore_apt_sources() {
  rm -f /etc/apt/sources.list.d/memgraph-offline.list
  rmdir /etc/apt/sources.list.d 2>/dev/null || true
  [[ -d "$SOURCES_D_BACKUP" ]] && mv "$SOURCES_D_BACKUP" /etc/apt/sources.list.d || true
  [[ -f "$SOURCES_BACKUP" ]] && mv "$SOURCES_BACKUP" /etc/apt/sources.list || true
}
trap restore_apt_sources EXIT

apt-get -o Acquire::Check-Valid-Until=false update

# Order matches mage/install_runtime_requirements.sh + the runtime control deps.
# Don't list dpkg-dev again (already installed); list everything else.
RUNTIME_PACKAGES=(
  libcurl4
  libpython3.12
  openssl
  python3
  python3-pip
  python3-setuptools
  adduser
  libgomp1
  libaio1t64
  libatomic1
  libkrb5-3
  libseccomp2
  libxmlsec1
  ca-certificates
  msodbcsql18
  unixodbc-dev
)

echo "msodbcsql18 msodbcsql/ACCEPT_EULA boolean true" | debconf-set-selections
ACCEPT_EULA=Y apt-get install -y --no-install-recommends "${RUNTIME_PACKAGES[@]}"

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

# Compose with the apt restore trap.
trap 'restore_pip_conf; restore_apt_sources' EXIT

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

# Use the local apt repo so the memgraph + mage debs' Depends: lines resolve
# without touching the network.
apt-get install -y --no-install-recommends "$MEMGRAPH_DEB" "$MAGE_DEB"

echo
echo "Memgraph + MAGE installed successfully."
echo "Start with: systemctl start memgraph"
