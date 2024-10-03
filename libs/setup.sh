#!/bin/bash -e

# Download external dependencies.
# Don't forget to add/update the license in release/third-party-licenses of added/updated libs!

local_cache_host=${MGDEPS_CACHE_HOST_PORT:-mgdeps-cache:8000}
working_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${working_dir}"

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
      git clone --depth 1 --branch "$checkout_id" "$git_repo" "$dir_name" || return 1
    else
      git clone "$git_repo" "$dir_name" || return 1
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
      timeout 15 wget -nv "$primary_url" -O "$filename" >/dev/null 2>&1 || wget -nv "$secondary_url" -O "$filename" || exit 1
    else
      echo "Download from $secondary_url"
      wget -nv "$secondary_url" -O "$filename" || exit 1
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

# NOTE: Antlr tag is exceptionally here because it's required on multiple
# places because of the additional required .jar file.
antlr4_tag="4.13.2" # 2024-08-03

declare -A primary_urls=(
  ["antlr4-code"]="http://$local_cache_host/git/antlr4.git"
  ["antlr4-generator"]="http://$local_cache_host/file/antlr-$antlr4_tag-complete.jar"
  ["cppitertools"]="http://$local_cache_host/git/cppitertools.git"
  ["rapidcheck"]="http://$local_cache_host/git/rapidcheck.git"
  ["gbenchmark"]="http://$local_cache_host/git/benchmark.git"
  ["gtest"]="http://$local_cache_host/git/googletest.git"
  ["libbcrypt"]="http://$local_cache_host/git/libbcrypt.git"
  ["rocksdb"]="http://$local_cache_host/git/rocksdb.git"
  ["mgclient"]="http://$local_cache_host/git/mgclient.git"
  ["pymgclient"]="http://$local_cache_host/git/pymgclient.git"
  ["mgconsole"]="http://$local_cache_host/git/mgconsole.git"
  ["spdlog"]="http://$local_cache_host/git/spdlog"
  ["nlohmann"]="http://$local_cache_host/file/nlohmann/json/4f8fba14066156b73f1189a2b8bd568bde5284c5/single_include/nlohmann/json.hpp"
  ["neo4j"]="http://$local_cache_host/file/neo4j-community-5.6.0-unix.tar.gz"
  ["librdkafka"]="http://$local_cache_host/git/librdkafka.git"
  ["protobuf"]="http://$local_cache_host/git/protobuf.git"
  ["pulsar"]="http://$local_cache_host/git/pulsar.git"
  ["librdtsc"]="http://$local_cache_host/git/librdtsc.git"
  ["ctre"]="http://$local_cache_host/file/hanickadot/compile-time-regular-expressions/v3.7.2/single-header/ctre.hpp"
  ["absl"]="http://$local_cache_host/git/abseil-cpp.git"
  ["jemalloc"]="http://$local_cache_host/git/jemalloc.git"
  ["range-v3"]="http://$local_cache_host/git/range-v3.git"
  ["nuraft"]="http://$local_cache_host/git/NuRaft.git"
  ["asio"]="http://$local_cache_host/git/asio.git"
  ["mgcxx"]="http://$local_cache_host/git/mgcxx.git"
  ["strong_type"]="http://$local_cache_host/git/strong_type.git"
)

# The goal of secondary urls is to have links to the "source of truth" of
# dependencies, e.g., Github or S3. Download from secondary urls, if happens
# at all, should never fail. In other words, if it fails, the whole build
# should fail.
declare -A secondary_urls=(
  ["antlr4-code"]="https://github.com/antlr/antlr4.git"
  ["antlr4-generator"]="https://www.antlr.org/download/antlr-$antlr4_tag-complete.jar"
  ["cppitertools"]="https://github.com/ryanhaining/cppitertools.git"
  ["rapidcheck"]="https://github.com/emil-e/rapidcheck.git"
  ["gbenchmark"]="https://github.com/google/benchmark.git"
  ["gtest"]="https://github.com/google/googletest.git"
  ["libbcrypt"]="https://github.com/rg3/libbcrypt"
  ["rocksdb"]="https://github.com/facebook/rocksdb.git"
  ["mgclient"]="https://github.com/memgraph/mgclient.git"
  ["pymgclient"]="https://github.com/memgraph/pymgclient.git"
  ["mgconsole"]="https://github.com/memgraph/mgconsole.git"
  ["spdlog"]="https://github.com/gabime/spdlog"
  ["nlohmann"]="https://raw.githubusercontent.com/nlohmann/json/4f8fba14066156b73f1189a2b8bd568bde5284c5/single_include/nlohmann/json.hpp"
  ["neo4j"]="https://dist.neo4j.org/neo4j-community-5.6.0-unix.tar.gz"
  ["librdkafka"]="https://github.com/edenhill/librdkafka.git"
  ["protobuf"]="https://github.com/protocolbuffers/protobuf.git"
  ["pulsar"]="https://github.com/apache/pulsar.git"
  ["librdtsc"]="https://github.com/gabrieleara/librdtsc.git"
  ["ctre"]="https://raw.githubusercontent.com/hanickadot/compile-time-regular-expressions/v3.7.2/single-header/ctre.hpp"
  ["absl"]="https://github.com/abseil/abseil-cpp.git"
  ["jemalloc"]="https://github.com/jemalloc/jemalloc.git"
  ["range-v3"]="https://github.com/ericniebler/range-v3.git"
  ["nuraft"]="https://github.com/eBay/NuRaft.git"
  ["asio"]="https://github.com/chriskohlhoff/asio.git"
  ["mgcxx"]="https://github.com/memgraph/mgcxx.git"
  ["strong_type"]="https://github.com/rollbear/strong_type.git"
)

# antlr
file_get_try_double "${primary_urls[antlr4-generator]}" "${secondary_urls[antlr4-generator]}"
repo_clone_try_double "${primary_urls[antlr4-code]}" "${secondary_urls[antlr4-code]}" "antlr4" "$antlr4_tag" true

cppitertools_ref="v2.1" # 2021-01-15
repo_clone_try_double "${primary_urls[cppitertools]}" "${secondary_urls[cppitertools]}" "cppitertools" "$cppitertools_ref"

# rapidcheck
rapidcheck_tag="1c91f40e64d87869250cfb610376c629307bf77d" # (2023-08-15)
repo_clone_try_double "${primary_urls[rapidcheck]}" "${secondary_urls[rapidcheck]}" "rapidcheck" "$rapidcheck_tag"

# google benchmark
benchmark_tag="v1.6.0"
repo_clone_try_double "${primary_urls[gbenchmark]}" "${secondary_urls[gbenchmark]}" "benchmark" "$benchmark_tag" true

# google test
googletest_tag="v1.14.0"
repo_clone_try_double "${primary_urls[gtest]}" "${secondary_urls[gtest]}" "googletest" "$googletest_tag" true

# libbcrypt
libbcrypt_tag="8aa32ad94ebe06b76853b0767c910c9fbf7ccef4" # custom version (Dec 16, 2016)
repo_clone_try_double "${primary_urls[libbcrypt]}" "${secondary_urls[libbcrypt]}" "libbcrypt" "$libbcrypt_tag"

# neo4j
file_get_try_double "${primary_urls[neo4j]}" "${secondary_urls[neo4j]}"
tar -xzf neo4j-community-5.6.0-unix.tar.gz
mv neo4j-community-5.6.0 neo4j
rm neo4j-community-5.6.0-unix.tar.gz

# nlohmann json
# We wget header instead of cloning repo since repo is huge (lots of test data).
# We use head on Sep 1, 2017 instead of last release since it was long time ago.
mkdir -p json
cd json
file_get_try_double "${primary_urls[nlohmann]}" "${secondary_urls[nlohmann]}"
cd ..

rocksdb_tag="v8.1.1" # (2023-04-21)
repo_clone_try_double "${primary_urls[rocksdb]}" "${secondary_urls[rocksdb]}" "rocksdb" "$rocksdb_tag" true
pushd rocksdb
git apply ../rocksdb8.1.1.patch
popd

# mgclient
mgclient_tag="v1.4.0" # (2022-06-14)
repo_clone_try_double "${primary_urls[mgclient]}" "${secondary_urls[mgclient]}" "mgclient" "$mgclient_tag"
sed -i 's/\${CMAKE_INSTALL_LIBDIR}/lib/' mgclient/src/CMakeLists.txt

# pymgclient
pymgclient_tag="4f85c179e56302d46a1e3e2cf43509db65f062b3" # (2021-01-15)
repo_clone_try_double "${primary_urls[pymgclient]}" "${secondary_urls[pymgclient]}" "pymgclient" "$pymgclient_tag"

# mgconsole
mgconsole_tag="v1.4.0" # (2023-05-21)
repo_clone_try_double "${primary_urls[mgconsole]}" "${secondary_urls[mgconsole]}" "mgconsole" "$mgconsole_tag" true

spdlog_tag="v1.12.0" # (2022-11-02)
repo_clone_try_double "${primary_urls[spdlog]}" "${secondary_urls[spdlog]}" "spdlog" "$spdlog_tag" true

# librdkafka
librdkafka_tag="v1.7.0" # (2021-05-06)
repo_clone_try_double "${primary_urls[librdkafka]}" "${secondary_urls[librdkafka]}" "librdkafka" "$librdkafka_tag" true

# protobuf
protobuf_tag="v3.12.4"
repo_clone_try_double "${primary_urls[protobuf]}" "${secondary_urls[protobuf]}" "protobuf" "$protobuf_tag" true
pushd protobuf
./autogen.sh && ./configure CC=clang CXX=clang++ --prefix=$(pwd)/lib
popd

#pulsar
pulsar_tag="v2.8.1"
repo_clone_try_double "${primary_urls[pulsar]}" "${secondary_urls[pulsar]}" "pulsar" "$pulsar_tag" true
pushd pulsar
git apply ../pulsar.patch
popd

#librdtsc
librdtsc_tag="v0.3"
repo_clone_try_double "${primary_urls[librdtsc]}" "${secondary_urls[librdtsc]}" "librdtsc" "$librdtsc_tag" true
pushd librdtsc
git apply ../librdtsc.patch
popd

#ctre
mkdir -p ctre
cd ctre
file_get_try_double "${primary_urls[ctre]}" "${secondary_urls[ctre]}"
cd ..

# abseil 20240116.2
absl_ref="20240116.2"
repo_clone_try_double "${primary_urls[absl]}" "${secondary_urls[absl]}" "absl" "$absl_ref"

# jemalloc ea6b3e973b477b8061e0076bb257dbd7f3faa756
JEMALLOC_COMMIT_VERSION="5.2.1"
repo_clone_try_double "${primary_urls[jemalloc]}" "${secondary_urls[jemalloc]}" "jemalloc" "$JEMALLOC_COMMIT_VERSION"

# this is hack for cmake in libs to set path, and for FindJemalloc to use Jemalloc_INCLUDE_DIR
pushd jemalloc

./autogen.sh
MALLOC_CONF="background_thread:true,retain:false,percpu_arena:percpu,oversize_threshold:0,muzzy_decay_ms:5000,dirty_decay_ms:5000" \
./configure \
  --disable-cxx \
  --with-lg-page=12 \
  --with-lg-hugepage=21 \
  --enable-shared=no --prefix=$working_dir \
  --with-malloc-conf="background_thread:true,retain:false,percpu_arena:percpu,oversize_threshold:0,muzzy_decay_ms:5000,dirty_decay_ms:5000"

make -j$CPUS install
popd

#range-v3 release-0.12.0
range_v3_ref="release-0.12.0"
repo_clone_try_double "${primary_urls[range-v3]}" "${secondary_urls[range-v3]}" "rangev3" "$range_v3_ref"

# Asio
asio_tag="asio-1-24-0"
repo_clone_try_double "${primary_urls[asio]}" "${secondary_urls[asio]}" "asio" "$asio_tag" true

# NuRaft
NURAFT_COMMIT_HASH="4b148a7e76291898c838a7457eeda2b16f7317ea"
NURAFT_TAG="master"
repo_clone_try_double "${primary_urls[nuraft]}" "${secondary_urls[nuraft]}" "nuraft" "$NURAFT_TAG" false
pushd nuraft
mv ../asio .
git checkout $NURAFT_COMMIT_HASH
git apply ../nuraft.patch
./prepare.sh
popd

# mgcxx (text search)
mgcxx_tag="v0.0.7"
repo_clone_try_double "${primary_urls[mgcxx]}" "${secondary_urls[mgcxx]}" "mgcxx" "$mgcxx_tag" true

# strong_type v14
strong_type_ref="v14"
repo_clone_try_double "${primary_urls[strong_type]}" "${secondary_urls[strong_type]}" "strong_type" "$strong_type_ref"
