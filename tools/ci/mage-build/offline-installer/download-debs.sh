#!/bin/bash
set -euo pipefail
# Run me inside a clean ubuntu:24.04 container as root.
# Downloads (without installing on the target) every .deb required to bring a
# vanilla ubuntu:24.04 host up to the level of dependencies that memgraph +
# MAGE need. Strategy: snapshot installed packages before/after `apt install`,
# then download .debs for the delta — that's the exact closure of packages
# that need to ship in the bundle, including transitive deps that were
# pre-installed in the build container and would otherwise be missing.
# Output: /output/debs/*.deb

OUTPUT_DIR=${OUTPUT_DIR:-/output/debs}

# Packages required at runtime by memgraph + MAGE on Ubuntu 24.04.
# Sourced from:
#   - mage/install_runtime_requirements.sh
#   - CPACK_DEBIAN_MAGE_PACKAGE_DEPENDS in release/CMakeLists.txt (memgraph-mage Depends)
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
  libdw1t64
)

# msodbcsql18 comes from the Microsoft apt source. Pull it in too so the
# pyodbc-based modules work offline.
MSSQL_PACKAGES=(
  msodbcsql18
  unixodbc-dev
)

mkdir -p "$OUTPUT_DIR"

export DEBIAN_FRONTEND=noninteractive

# Snapshot the base ubuntu:24.04 package set BEFORE we install anything. The
# delta after install is the exact set of packages a fresh ubuntu:24.04 host
# is missing, which is what the offline installer must ship.
BASE_PKGS=$(mktemp)
dpkg-query -W -f '${Package}\n' | sort -u > "$BASE_PKGS"

apt-get update

# bootstrap deps that the script itself needs (not the target):
#   - curl, gnupg, ca-certificates: fetch the microsoft apt source
#   - apt-utils: silences the "delaying package configuration" debconf noise
# These get rolled into the install closure; we strip them back out at the end.
apt-get install -y --no-install-recommends curl gnupg ca-certificates apt-utils

# Microsoft repo for msodbcsql18.
curl -sSL -O https://packages.microsoft.com/config/ubuntu/24.04/packages-microsoft-prod.deb
dpkg -i packages-microsoft-prod.deb
rm -f packages-microsoft-prod.deb
apt-get update

# Pre-accept the EULA for msodbcsql18 so its postinst doesn't block.
echo "msodbcsql18 msodbcsql/ACCEPT_EULA boolean true" | debconf-set-selections
export ACCEPT_EULA=Y

# Actually install (not just download) so apt resolves the entire transitive
# dependency closure. We collect the resulting deltas in a moment.
apt-get install -y --no-install-recommends \
  "${RUNTIME_PACKAGES[@]}" \
  "${MSSQL_PACKAGES[@]}"

# Compute the delta: packages installed *because of* our request, minus the
# base image set. Exclude bootstrap packages (curl/gnupg/etc.) that we only
# needed in this container; they're not required on the target.
ALL_PKGS=$(mktemp)
DELTA_PKGS=$(mktemp)
EXCLUDE=$(mktemp)
dpkg-query -W -f '${Package}\n' | sort -u > "$ALL_PKGS"
printf '%s\n' curl gnupg apt-utils | sort -u > "$EXCLUDE"
comm -23 <(comm -13 "$BASE_PKGS" "$ALL_PKGS") "$EXCLUDE" > "$DELTA_PKGS"

# The snapshot-diff captures packages that are *new* on the target relative
# to ubuntu:24.04 base. But several debs in the delta carry strict-version
# Depends on base packages (e.g. libatomic1 requires `gcc-14-base (= X.Y)`,
# libc6-dev requires `libc6 (= X.Y)`). If the target's base image is a patch
# release older than ours, those strict deps fail. Walk the strict-version
# Depends/Pre-Depends of the delta — and any newly-added base packages —
# until we reach a fixed point, then download everything we found.
TO_SHIP=$(mktemp)
PREV=$(mktemp)
sort -u "$DELTA_PKGS" > "$TO_SHIP"
while true; do
  cp "$TO_SHIP" "$PREV"
  # `dpkg-query -W -f 'Pre-Depends:\nDepends:'` returns both fields per pkg.
  # Split on commas, keep only `(= version)` constraints (strict pins —
  # the ones that fail when the target's base packages are slightly older),
  # and extract the package name.
  STRICT=$(xargs -a "$TO_SHIP" -r dpkg-query -W \
             -f '${Pre-Depends}\n${Depends}\n' 2>/dev/null \
           | tr ',' '\n' \
           | grep -E '\([[:space:]]*=' \
           | sed -E 's/^[[:space:]]*([A-Za-z0-9._+-]+).*/\1/' \
           | sort -u)
  printf '%s\n' "$STRICT" | sort -u | grep -v '^$' \
    | cat - "$TO_SHIP" | sort -u > "$TO_SHIP.new"
  mv "$TO_SHIP.new" "$TO_SHIP"
  cmp -s "$PREV" "$TO_SHIP" && break
done

echo "Downloading $(wc -l < "$TO_SHIP") packages to $OUTPUT_DIR (incl. strict-version base deps)"
cd "$OUTPUT_DIR"
xargs -a "$TO_SHIP" -n1 apt-get download

echo "Downloaded $(ls "$OUTPUT_DIR" | wc -l) .deb files"
