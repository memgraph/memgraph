#!/bin/bash -e

# Download external dependencies.
# Don't forget to add/update the license in release/third-party-licenses of added/updated libs!

local_cache_host=${MGDEPS_CACHE_HOST_PORT:-mgdeps-cache:8000}
working_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${working_dir}"
WGET_OR_CLONE_TIMEOUT=60

#  if the toolchain hasn't been activated, activate it
deactivate_toolchain=false
if [ -z "$MG_TOOLCHAIN_VERSION" ]; then
  echo "Activating toolchain"
  source /opt/toolchain-v7/activate
  deactivate_toolchain=true
fi

function print_help () {
    echo "Usage: $0 [OPTION]"
    echo -e "Setup libs for the project.\n"
    echo "Optonal environment variables:"
    echo -e "  MGDEPS_CACHE_HOST_PORT\thost and port for mgdeps cache service, set host to 'false' to not use mgdeps-cache (default: mgdeps-cache:8000) "
}

use_cache=true
if [[ "${local_cache_host%:*}" == "false" ]]; then
  use_cache=false
  echo -e "\n--- Not using cache ---\n"
fi

if [[ $# -eq 1 && "$1" == "-h" ]]; then
    print_help
    exit 0
fi

# Clones a git repository and optionally cherry picks additional commits. The
# function will try to preserve any local changes in the repo.
# clone GIT_REPO DIR_NAME CHECKOUT_ID [CHERRY_PICK_ID]...
clone () {
  local git_repo=$1
  local dir_name=$2
  local checkout_id=$3
  local shallow=$4

  shift 4
  # Clone if there's no repo.
  if [[ ! -d "$dir_name" ]]; then
    echo "Cloning from $git_repo"
    # If the clone fails, it doesn't make sense to continue with the function
    # execution but the whole script should continue executing because we might
    # clone the same repo from a different source.

    if [ "$shallow" = true ]; then
      timeout $WGET_OR_CLONE_TIMEOUT git clone --depth 1 --branch "$checkout_id" "$git_repo" "$dir_name" || return 1
    else
      timeout $WGET_OR_CLONE_TIMEOUT git clone "$git_repo" "$dir_name" || return 1
    fi
  fi
  pushd "$dir_name"
  # Check whether we have any local changes which need to be preserved.
  local local_changes=true
  if git diff --no-ext-diff --quiet && git diff --no-ext-diff --cached --quiet; then
    local_changes=false
  fi

  if [ "$shallow" = false ]; then
    # Stash regardless of local_changes, so that a user gets a message on stdout.
    git stash
    # Just fetch new commits from remote repository. Don't merge/pull them in, so
    # that we don't clobber local modifications.
    git fetch
    # Checkout the primary commit (there's no need to pull/merge).
    # The checkout fail should exit this script immediately because the target
    # commit is not there and that will most likely create build-time errors.
    git checkout "$checkout_id" || exit 1
    # Apply any optional cherry pick fixes.
    while [[ $# -ne 0 ]]; do
      local cherry_pick_id=$1
      shift
      # The cherry-pick fail should exit this script immediately because the
      # target commit is not there and that will most likely create build-time
      # errors.
      git cherry-pick -n "$cherry_pick_id" || exit 1
    done
  fi

  # Reapply any local changes.
  if [[ $local_changes == true ]]; then
    git stash pop
  fi
  popd
}

file_get_try_double () {
    primary_url="$1"
    secondary_url="$2"
    if [ -z "$primary_url" ]; then echo "Primary should not be empty." && exit 1; fi
    if [ -z "$secondary_url" ]; then echo "Secondary should not be empty." && exit 1; fi
    filename="$(basename "$secondary_url")"
    if [[ "$use_cache" == true ]]; then
      echo "Download primary from $primary_url secondary from $secondary_url"
      # Redirect primary/cache to /dev/null to make it less confusing for a new contributor because only CI has access to the cache.
      timeout $WGET_OR_CLONE_TIMEOUT wget -nv "$primary_url" -O "$filename" >/dev/null 2>&1 || timeout $WGET_OR_CLONE_TIMEOUT wget -nv "$secondary_url" -O "$filename" || exit 1
    else
      echo "Download from $secondary_url"
      timeout $WGET_OR_CLONE_TIMEOUT wget -nv "$secondary_url" -O "$filename" || exit 1
    fi
}

repo_clone_try_double () {
    primary_url="$1"
    secondary_url="$2"
    folder_name="$3"
    ref="$4"
    shallow="${5:-false}"
    if [ -z "$primary_url" ]; then echo "Primary should not be empty." && exit 1; fi
    if [ -z "$secondary_url" ]; then echo "Secondary should not be empty." && exit 1; fi
    if [ -z "$folder_name" ]; then echo "Clone folder should not be empty." && exit 1; fi
    if [ -z "$ref" ]; then echo "Git clone ref should not be empty." && exit 1; fi
    if [[ "$use_cache" == true ]]; then
      echo "Cloning primary from $primary_url secondary from $secondary_url"
      # Redirect primary/cache to /dev/null to make it less confusing for a new contributor because only CI has access to the cache.
      clone "$primary_url" "$folder_name" "$ref" "$shallow" >/dev/null 2>&1 || clone "$secondary_url" "$folder_name" "$ref" "$shallow" || exit 1
    else
      echo "Cloning from $secondary_url"
      clone "$secondary_url" "$folder_name" "$ref" "$shallow" || exit 1
    fi
}

# List all dependencies.

# The reason for introducing primary and secondary urls are:
#   * HTTPS is hard to cache
#   * Remote development workflow is more flexible if people don't have to connect to VPN
#   * Direct download from the "source of truth" is slower and unreliable because of the whole internet in-between
#   * When a new dependency has to be added, both urls could be the same, later someone could optimize if required

# The goal of having primary urls is to have links to the "local" cache of
# dependencies where these dependencies could be downloaded as fast as
# possible. The actual cache server could be on your local machine, on a
# dedicated machine inside the build cluster or on the actual build machine.
# Download from primary_urls might fail because the cache is not installed.

declare -A primary_urls=(
  ["rapidcheck"]="http://$local_cache_host/git/rapidcheck.git"
  ["libbcrypt"]="http://$local_cache_host/git/libbcrypt.git"
  ["mgconsole"]="http://$local_cache_host/git/mgconsole.git"
  ["neo4j"]="http://$local_cache_host/file/neo4j-community-5.6.0-unix.tar.gz"
  ["librdkafka"]="http://$local_cache_host/git/librdkafka.git"
  ["protobuf"]="http://$local_cache_host/git/protobuf.git"
  ["pulsar"]="http://$local_cache_host/git/pulsar.git"
  ["librdtsc"]="http://$local_cache_host/git/librdtsc.git"
  ["jemalloc"]="http://$local_cache_host/git/jemalloc.git"
  ["nuraft"]="http://$local_cache_host/git/NuRaft.git"
  ["mgcxx"]="http://$local_cache_host/git/mgcxx.git"
  ["usearch"]="http://$local_cache_host/git/usearch.git"
)

# The goal of secondary urls is to have links to the "source of truth" of
# dependencies, e.g., Github or S3. Download from secondary urls, if happens
# at all, should never fail. In other words, if it fails, the whole build
# should fail.
declare -A secondary_urls=(
  ["rapidcheck"]="https://github.com/emil-e/rapidcheck.git"
  ["libbcrypt"]="https://github.com/rg3/libbcrypt"
  ["mgconsole"]="https://github.com/memgraph/mgconsole.git"
  ["neo4j"]="https://dist.neo4j.org/neo4j-community-5.6.0-unix.tar.gz"
  ["librdkafka"]="https://github.com/edenhill/librdkafka.git"
  ["protobuf"]="https://github.com/protocolbuffers/protobuf.git"
  ["pulsar"]="https://github.com/apache/pulsar.git"
  ["librdtsc"]="https://github.com/gabrieleara/librdtsc.git"
  ["jemalloc"]="https://github.com/jemalloc/jemalloc.git"
  ["nuraft"]="https://github.com/eBay/NuRaft.git"
  ["mgcxx"]="https://github.com/memgraph/mgcxx.git"
  ["usearch"]="https://github.com/unum-cloud/usearch.git"
)

# Skip download if we are under the latest toolchains (>= 6).
# IMPORTANT: If you want to use the latest version of a lib, just remove the
# function name in front of the download/clone call. To make the library work,
# you also have to adjust the CMake setup.
skip_if_under_toolchain () {
  artifact_name=$1 && shift 1
  if [ -z "${MG_TOOLCHAIN_VERSION}" ]; then
    $@
  else
    echo "Skipping $artifact_name download because it's already under the toolchain v$MG_TOOLCHAIN_VERSION"
  fi
}

# TODO (matt): parallelize this

# rapidcheck
rapidcheck_tag="ff6af6fc683159deb51c543b065eba14dfcf329b" # (2023-12-14)
repo_clone_try_double "${primary_urls[rapidcheck]}" "${secondary_urls[rapidcheck]}" "rapidcheck" "$rapidcheck_tag"

libbcrypt_tag="8aa32ad94ebe06b76853b0767c910c9fbf7ccef4" # custom version (Dec 16, 2016)
skip_if_under_toolchain "libbcrypt" repo_clone_try_double "${primary_urls[libbcrypt]}" "${secondary_urls[libbcrypt]}" "libbcrypt" "$libbcrypt_tag"

# neo4j
file_get_try_double "${primary_urls[neo4j]}" "${secondary_urls[neo4j]}"
tar -xzf neo4j-community-5.6.0-unix.tar.gz
mv neo4j-community-5.6.0 neo4j
rm neo4j-community-5.6.0-unix.tar.gz

# rocksdb_tag="v9.10.0" # (2023-04-21)
# repo_clone_try_double "${primary_urls[rocksdb]}" "${secondary_urls[rocksdb]}" "rocksdb" "$rocksdb_tag" true
# pushd rocksdb
# git apply ../rocksdb9.10.0.patch
# popd

# nlohmann json
# We wget header instead of cloning repo since repo is huge (lots of test data).
# We use head on Sep 1, 2017 instead of last release since it was long time ago.
# mkdir -p json
# cd json
# file_get_try_double "${primary_urls[nlohmann]}" "${secondary_urls[nlohmann]}"
# cd ..

# rocksdb_tag="v8.1.1" # (2023-04-21)
# repo_clone_try_double "${primary_urls[rocksdb]}" "${secondary_urls[rocksdb]}" "rocksdb" "$rocksdb_tag" true
# pushd rocksdb
# git apply ../rocksdb8.1.1.patch
# popd

# mgconsole
mgconsole_tag="v1.4.0" # (2023-05-21)
skip_if_under_toolchain "mgconsole" repo_clone_try_double "${primary_urls[mgconsole]}" "${secondary_urls[mgconsole]}" "mgconsole" "$mgconsole_tag" true

# librdkafka
librdkafka_tag="v2.6.1" # (2024-12-05)
skip_if_under_toolchain "librdkafka" repo_clone_try_double "${primary_urls[librdkafka]}" "${secondary_urls[librdkafka]}" "librdkafka" "$librdkafka_tag" true

if [ -z "${MG_TOOLCHAIN_VERSION}" ]; then
  protobuf_tag="v3.12.4"
  repo_clone_try_double "${primary_urls[protobuf]}" "${secondary_urls[protobuf]}" "protobuf" "$protobuf_tag" true
  pushd protobuf
  ./autogen.sh && ./configure CC=clang CXX=clang++ --prefix=$(pwd)/lib
  popd
else
  echo "Skipping protobuf download because it's already under the toolchain v$MG_TOOLCHAIN_VERSION"
fi

if [ -z "${MG_TOOLCHAIN_VERSION}" ]; then
  pulsar_tag="v2.8.1"
  repo_clone_try_double "${primary_urls[pulsar]}" "${secondary_urls[pulsar]}" "pulsar" "$pulsar_tag" true
  pushd pulsar
  git apply ../pulsar.patch
  popd
else
  echo "Skipping pulsar download because it's already under the toolchain v$MG_TOOLCHAIN_VERSION"
fi

if [ -z "${MG_TOOLCHAIN_VERSION}" ]; then
  librdtsc_tag="v0.3"
  repo_clone_try_double "${primary_urls[librdtsc]}" "${secondary_urls[librdtsc]}" "librdtsc" "$librdtsc_tag" true
  pushd librdtsc
  git apply ../librdtsc.patch
  popd
else
  echo "Skipping librdtsc download because it's already under the toolchain v$MG_TOOLCHAIN_VERSION"
fi

# jemalloc ea6b3e973b477b8061e0076bb257dbd7f3faa756
JEMALLOC_COMMIT_VERSION="5.2.1"
repo_clone_try_double "${primary_urls[jemalloc]}" "${secondary_urls[jemalloc]}" "jemalloc" "$JEMALLOC_COMMIT_VERSION"
# this is hack for cmake in libs to set path, and for FindJemalloc to use Jemalloc_INCLUDE_DIR
pushd jemalloc
./autogen.sh
export CC=clang
./configure \
  --disable-cxx \
  --with-lg-page=12 \
  --with-lg-hugepage=21 \
  --enable-shared=no --prefix=$working_dir \
  --with-jemalloc-prefix=je_ \
  --with-malloc-conf="background_thread:true,retain:false,percpu_arena:percpu,oversize_threshold:0,muzzy_decay_ms:5000,dirty_decay_ms:5000"
make -j$CPUS install
popd

# mgcxx (text search)
mgcxx_tag="v0.0.10"
repo_clone_try_double "${primary_urls[mgcxx]}" "${secondary_urls[mgcxx]}" "mgcxx" "$mgcxx_tag" true

# usearch (shallow clone to reduce flakiness)
usearch_ref="v2.15.3"
repo_clone_try_double "${primary_urls[usearch]}" "${secondary_urls[usearch]}" "usearch" "$usearch_ref" true
pushd usearch
git submodule update --init --recursive
popd


if [[ "$deactivate_toolchain" == "true" ]]; then
  echo "Deactivating toolchain"
  deactivate
fi
