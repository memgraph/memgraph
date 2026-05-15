#!/bin/bash
set -euo pipefail
# Run me inside a clean ubuntu:24.04 container as root.
# Downloads (without installing) every .deb required to bring a vanilla
# ubuntu:24.04 host up to the level of dependencies that memgraph + MAGE need,
# plus dpkg-dev so the installer can build a tiny local apt repo on the target.
# Output: /output/debs/*.deb

OUTPUT_DIR=${OUTPUT_DIR:-/output/debs}

# Packages required at runtime by memgraph + MAGE on Ubuntu 24.04.
# Sourced from:
#   - mage/install_runtime_requirements.sh
#   - tools/ci/mage-build/package/debian/control (memgraph-mage Depends)
#   - release/CMakeLists.txt (memgraph CPACK_DEBIAN_PACKAGE_DEPENDS + shlibdeps)
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
)

# msodbcsql18 comes from the Microsoft apt source. Pull it in too so the
# pyodbc-based modules work offline.
MSSQL_PACKAGES=(
  msodbcsql18
  unixodbc-dev
)

# dpkg-dev gives us dpkg-scanpackages on the target so install.sh can build
# a local apt repo from the bundled debs without needing network access.
INSTALLER_TOOLS=(
  dpkg-dev
)

mkdir -p "$OUTPUT_DIR"

export DEBIAN_FRONTEND=noninteractive

apt-get update

# Microsoft repo for msodbcsql18. ca-certificates is required so curl can
# verify the HTTPS cert on packages.microsoft.com — the ubuntu:24.04 base
# image does not ship one.
apt-get install -y --no-install-recommends curl gnupg ca-certificates
curl -sSL -O https://packages.microsoft.com/config/ubuntu/24.04/packages-microsoft-prod.deb
dpkg -i packages-microsoft-prod.deb
rm -f packages-microsoft-prod.deb
apt-get update

# Pre-accept the EULA for msodbcsql18 so we can resolve it.
echo "msodbcsql18 msodbcsql/ACCEPT_EULA boolean true" | debconf-set-selections

# --download-only places .debs into /var/cache/apt/archives/. --reinstall makes
# it also download already-installed base packages, which is what we want so
# the bundle works on minimal hosts that may be missing some of them.
apt-get clean
apt-get install -y --no-install-recommends --download-only --reinstall \
  "${RUNTIME_PACKAGES[@]}" \
  "${MSSQL_PACKAGES[@]}" \
  "${INSTALLER_TOOLS[@]}"

cp -v /var/cache/apt/archives/*.deb "$OUTPUT_DIR/"

echo "Downloaded $(ls "$OUTPUT_DIR" | wc -l) .deb files to $OUTPUT_DIR"
