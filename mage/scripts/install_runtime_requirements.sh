#!/bin/bash
set -euo pipefail
# run me as `root` user

PY_VERSION=3.12
TARGET_ARCH=amd64
CI=false
CUDA=false
CUSTOM_MIRROR=
while [[ $# -gt 0 ]]; do
    case $1 in
        --py-version)
            PY_VERSION=$2
            shift 2
            ;;
        --target-arch)
            TARGET_ARCH=$2
            shift 2
            ;;
        --custom-mirror)
            CUSTOM_MIRROR=$2
            shift 2
            ;;
        --ci)
            CI=true
            shift 1
            ;;
        --cuda)
            CUDA=$2
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
    esac
done

# This modifies the apt configuration to retry 3 times
if [[ "$CI" == true ]]; then
    echo 'Acquire::Retries "3";' > /etc/apt/apt.conf.d/80-retries
fi

# We test for the fastest responding mirror in CI, we can set it here.
if [[ -n "$CUSTOM_MIRROR" && "$CI" == true ]]; then
    sed -E -i \
        -e '/^URIs:/ s#https?://[^ ]*archive\.ubuntu\.com#'"$CUSTOM_MIRROR"'#g' \
        -e '/^URIs:/ s#https?://security\.ubuntu\.com#'"$CUSTOM_MIRROR"'#g' \
        /etc/apt/sources.list.d/ubuntu.sources
fi

apt-get update
apt-get install -y curl
curl -sSL -O https://packages.microsoft.com/config/ubuntu/24.04/packages-microsoft-prod.deb
dpkg -i packages-microsoft-prod.deb
rm packages-microsoft-prod.deb
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev
apt-get install -y libcurl4 libpython${PY_VERSION} openssl python3 python3-pip python3-setuptools \
    adduser libgomp1 libaio1t64 libatomic1 --no-install-recommends
ln -s /usr/bin/$(ls /usr/bin | grep perf) /usr/bin/perf

# In CI, we clean up the apt cache to save space in the Docker container.
if [[ "$CI" == true ]]; then
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
fi

if [[ "${TARGET_ARCH}" == "arm64" ]]; then
    ln -s /usr/lib/aarch64-linux-gnu/libaio.so.1t64 /usr/lib/aarch64-linux-gnu/libaio.so.1;
else
    ln -s /usr/lib/x86_64-linux-gnu/libaio.so.1t64 /usr/lib/x86_64-linux-gnu/libaio.so.1;
fi
