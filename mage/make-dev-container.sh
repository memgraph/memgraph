#!/bin/bash

################################################################################
# Use this script within the `prod` container to convert it to a dev container #
# Optionally skip toolchain download with --no-toolchain                       #
# run me as root!                                                              #
################################################################################

# install dependencies
apt-get update
apt-get install -y libpython${PY_VERSION:-$(python3 --version | sed 's/Python //')} \
  libcurl4 libssl-dev openssl build-essential cmake curl g++ python3  \
  python3-pip python3-setuptools python3-dev clang git unixodbc-dev \
  libboost-all-dev uuid-dev gdb procps libc6-dbg libxmlsec1-dev xmlsec1 \
  --no-install-recommends


# Function to detect architecture and set the appropriate toolchain URL
get_toolchain_url() {
  version=$1
  arch=$(uname -m)

  # Determine the file name based on the architecture.
  case "$arch" in
    x86_64)
      file="toolchain-v${version}-binaries-ubuntu-24.04-amd64.tar.gz"
      ;;
    aarch64)
      file="toolchain-v${version}-binaries-ubuntu-24.04-arm64.tar.gz"
      ;;
    *)
      echo "Unsupported architecture: $arch" >&2
      exit 1
      ;;
  esac

  # Construct the mgdeps-cache file URL.
  mgdeps_url="http://mgdeps-cache:8000/file/${file}"

  # Check if the file is available on the cache using a HEAD request.
  if curl --head --silent --fail "$mgdeps_url" -o /dev/null; then
    echo "$mgdeps_url"
  else
    # Fallback to the S3 URL if the cache doesn't have the file.
    s3_url="https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v${version}/${file}"
    echo "$s3_url"
  fi
}


toolchain_version=7
TOOLCHAIN_URL=$(get_toolchain_url "$toolchain_version")
echo "Downloading toolchain from: $TOOLCHAIN_URL"
# Download the toolchain using curl
curl -L "$TOOLCHAIN_URL" -o /toolchain.tar.gz
tar xzvfm /toolchain.tar.gz -C /opt


echo "Cloning MAGE repo commit/tag: $MAGE_COMMIT"
cd /root
git clone https://github.com/memgraph/mage.git --recurse-submodules
cd /root/mage

# Check if MAGE_COMMIT matches a version tag format: vX.Y or vX.Y.Z
if [[ "$MAGE_COMMIT" =~ ^v[0-9]+\.[0-9]+(\.[0-9]+)?$ ]]; then
    echo "Detected valid tag format. Checking out tag: $MAGE_COMMIT"
    git checkout "$MAGE_COMMIT"
else
    echo "Detected branch name. Checking out branch: $MAGE_COMMIT"
    git checkout "$MAGE_COMMIT"

    echo "Fetching latest main branch..."
    git fetch origin main

    echo "Merging origin/main into branch $MAGE_COMMIT..."
    git merge origin/main
fi

cd /

echo "Copying repo files to /mage"
# Copy files without overwriting existing ones
cp -r --update=none /root/mage/. /mage/

# Change ownership of everything in /mage to memgraph
chown -R memgraph: /mage/

# remove git repo from `/root`
rm -rf /root/mage

# install toolchain run dependencies
./mage/cpp/memgraph/environment/os/install_deps.sh install TOOLCHAIN_RUN_DEPS
# TODO(matt): figure out a good way of installing the same rust version, without
# also having to install the toolchain build-deps. Perhaps a new option
# MAGE_BUILD_DEPS?
#rustversion=$(cargo --version | sed 's/cargo //')

# install Rust as `memgraph` user - this is required for building mage
su - memgraph -c 'curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain 1.85 && echo "export PATH=\$HOME/.cargo/bin:\$PATH" >> $HOME/.bashrc'

# build everything again (because it isn't copied into the prod image)
build_cmd="source /opt/toolchain-v${toolchain_version}/activate && cd /mage && python3 /mage/setup build --cpp-build-flags CMAKE_BUILD_TYPE=${BUILD_TYPE}"
su - memgraph -c "bash -c '$build_cmd'"


echo "Done!"
