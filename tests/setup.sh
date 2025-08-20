#!/bin/bash
# shellcheck disable=1091
set -Eeuo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

# Read existing packages from requirements.txt
existing_packages=$(cat "$DIR/../src/auth/reference_modules/requirements.txt")

# Define additional packages from PIP_DEPS
PIP_DEPS=(
   "behave==1.2.6"
   "ldap3==2.6"
   "kafka-python==2.0.2"
   "requests==2.25.1"
   "neo4j==5.14.1"
   "parse==1.18.0"
   "parse-type==0.5.2"
   "pytest==7.3.2"
   "pyyaml==6.0.1"
   "six==1.17.0"
   "networkx==2.5.1"
   "gqlalchemy==1.8.0"
   "python3-saml==1.16.0"
   "setuptools==75.8.0"
   "pymgclient==1.3.1"
   "xmlsec==1.3.16"
   "pulsar-client==3.5.0"
)

# Combine existing packages with PIP_DEPS, avoiding duplicates
packages="$existing_packages"
for dep in "${PIP_DEPS[@]}"; do
    # Extract package name (before the ==)
    pkg_name=$(echo "$dep" | cut -d'=' -f1)
    # Check if package already exists in the list
    if ! echo "$packages" | grep -q "^${pkg_name}=="; then
        packages="$packages"$'\n'"$dep"
    fi
done

# Remove old and create a new virtualenv.
if [ -d ve3 ]; then
    rm -rf ve3
fi
uv venv ve3 || exit 1
set +u
source "ve3/bin/activate"
set -u
uv pip install $packages
deactivate

"$DIR"/e2e/graphql/setup.sh
