#!/bin/bash
# shellcheck disable=1091
set -Eeuo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

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
   "gqlalchemy==1.6.0"
   "python3-saml==1.16.0"
   "setuptools==75.8.0"
   "pymgclient==1.3.1"
)

# Remove old and create a new virtualenv.
if [ -d ve3 ]; then
    rm -rf ve3
fi
python3 -m venv ve3
set +u
source "ve3/bin/activate"
set -u
# https://docs.python.org/3/library/sys.html#sys.version_info
PYTHON_MINOR=$(python3 -c 'import sys; print(sys.version_info[:][1])')
# install xmlsec
pip --timeout 1000 install xmlsec==1.3.16
# install pulsar-client
pip --timeout 1000 install "pulsar-client==3.5.0"
for pkg in "${PIP_DEPS[@]}"; do
    pip --timeout 1000 install "$pkg"
done
# https://github.com/SAML-Toolkits/python3-saml?tab=readme-ov-file#note
pip --timeout 1000 install --upgrade lxml==5.2.1
pip --timeout 1000 install "networkx==2.5.1"
pip --timeout 1000 install -r "$DIR/../src/auth/reference_modules/requirements.txt"
deactivate

"$DIR"/e2e/graphql/setup.sh
