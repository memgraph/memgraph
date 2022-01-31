#!/bin/bash -e

# Download external dependencies.
# Don't forget to add/update the license in release/third-party-licenses of added/updated libs!

local_cache_host=${MGDEPS_CACHE_HOST_PORT:-mgdeps-cache:8000}
working_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${working_dir}"

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
    echo "Download primary from $primary_url secondary from $secondary_url"
    if [ -z "$primary_url" ]; then echo "Primary should not be empty." && exit 1; fi
    if [ -z "$secondary_url" ]; then echo "Secondary should not be empty." && exit 1; fi
    filename="$(basename "$secondary_url")"
    wget -nv "$primary_url" -O "$filename" || wget -nv "$secondary_url" -O "$filename" || exit 1
    echo ""
}

repo_clone_try_double () {
    primary_url="$1"
    secondary_url="$2"
    folder_name="$3"
    ref="$4"
    shallow="${5:-false}"
    echo "Cloning primary from $primary_url secondary from $secondary_url"
    if [ -z "$primary_url" ]; then echo "Primary should not be empty." && exit 1; fi
    if [ -z "$secondary_url" ]; then echo "Secondary should not be empty." && exit 1; fi
    if [ -z "$folder_name" ]; then echo "Clone folder should not be empty." && exit 1; fi
    if [ -z "$ref" ]; then echo "Git clone ref should not be empty." && exit 1; fi
    clone "$primary_url" "$folder_name" "$ref" "$shallow" || clone "$secondary_url" "$folder_name" "$ref" "$shallow" || exit 1
    echo ""
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
  ["antlr4-code"]="http://$local_cache_host/git/antlr4.git"
  ["antlr4-generator"]="http://$local_cache_host/file/antlr-4.9.2-complete.jar"
  ["cppitertools"]="http://$local_cache_host/git/cppitertools.git"
  ["fmt"]="http://$local_cache_host/git/fmt.git"
  ["rapidcheck"]="http://$local_cache_host/git/rapidcheck.git"
  ["gbenchmark"]="http://$local_cache_host/git/benchmark.git"
  ["gtest"]="http://$local_cache_host/git/googletest.git"
  ["gflags"]="http://$local_cache_host/git/gflags.git"
  ["libbcrypt"]="http://$local_cache_host/git/libbcrypt.git"
  ["bzip2"]="http://$local_cache_host/git/bzip2.git"
  ["zlib"]="http://$local_cache_host/git/zlib.git"
  ["rocksdb"]="http://$local_cache_host/git/rocksdb.git"
  ["mgclient"]="http://$local_cache_host/git/mgclient.git"
  ["pymgclient"]="http://$local_cache_host/git/pymgclient.git"
  ["mgconsole"]="http://$local_cache_host/git/mgconsole.git"
  ["spdlog"]="http://$local_cache_host/git/spdlog"
  ["jemalloc"]="http://$local_cache_host/git/jemalloc.git"
  ["nlohmann"]="http://$local_cache_host/file/nlohmann/json/4f8fba14066156b73f1189a2b8bd568bde5284c5/single_include/nlohmann/json.hpp"
  ["neo4j"]="http://$local_cache_host/file/neo4j-community-3.2.3-unix.tar.gz"
  ["librdkafka"]="http://$local_cache_host/git/librdkafka.git"
  ["protobuf"]="http://$local_cache_host/git/protobuf.git"
  ["boost"]="http://$local_cache_host/file/boost_1_77_0.tar.gz"
  ["pulsar"]="http://$local_cache_host/git/pulsar.git"
  ["librdtsc"]="http://$local_cache_host/git/librdtsc.git"
)

# The goal of secondary urls is to have links to the "source of truth" of
# dependencies, e.g., Github or S3. Download from secondary urls, if happens
# at all, should never fail. In other words, if it fails, the whole build
# should fail.
declare -A secondary_urls=(
  ["antlr4-code"]="https://github.com/antlr/antlr4.git"
  ["antlr4-generator"]="http://www.antlr.org/download/antlr-4.9.2-complete.jar"
  ["cppitertools"]="https://github.com/ryanhaining/cppitertools.git"
  ["fmt"]="https://github.com/fmtlib/fmt.git"
  ["rapidcheck"]="https://github.com/emil-e/rapidcheck.git"
  ["gbenchmark"]="https://github.com/google/benchmark.git"
  ["gtest"]="https://github.com/google/googletest.git"
  ["gflags"]="https://github.com/memgraph/gflags.git"
  ["libbcrypt"]="https://github.com/rg3/libbcrypt"
  ["bzip2"]="https://github.com/VFR-maniac/bzip2"
  ["zlib"]="https://github.com/madler/zlib.git"
  ["rocksdb"]="https://github.com/facebook/rocksdb.git"
  ["mgclient"]="https://github.com/memgraph/mgclient.git"
  ["pymgclient"]="https://github.com/memgraph/pymgclient.git"
  ["mgconsole"]="http://github.com/memgraph/mgconsole.git"
  ["spdlog"]="https://github.com/gabime/spdlog"
  ["jemalloc"]="https://github.com/jemalloc/jemalloc.git"
  ["nlohmann"]="https://raw.githubusercontent.com/nlohmann/json/4f8fba14066156b73f1189a2b8bd568bde5284c5/single_include/nlohmann/json.hpp"
  ["neo4j"]="https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/neo4j-community-3.2.3-unix.tar.gz"
  ["librdkafka"]="https://github.com/edenhill/librdkafka.git"
  ["protobuf"]="https://github.com/protocolbuffers/protobuf.git"
  ["boost"]="https://boostorg.jfrog.io/artifactory/main/release/1.77.0/source/boost_1_77_0.tar.gz"
  ["pulsar"]="https://github.com/apache/pulsar.git"
  ["librdtsc"]="https://github.com/gabrieleara/librdtsc.git"
)

# antlr
file_get_try_double "${primary_urls[antlr4-generator]}" "${secondary_urls[antlr4-generator]}"

antlr4_tag="4.9.2" # v4.9.2
repo_clone_try_double "${primary_urls[antlr4-code]}" "${secondary_urls[antlr4-code]}" "antlr4" "$antlr4_tag" true
pushd antlr4
git apply ../antlr4.patch
popd

# cppitertools v2.0 2019-12-23
cppitertools_ref="cb3635456bdb531121b82b4d2e3afc7ae1f56d47"
repo_clone_try_double "${primary_urls[cppitertools]}" "${secondary_urls[cppitertools]}" "cppitertools" "$cppitertools_ref"

# rapidcheck
rapidcheck_tag="7bc7d302191a4f3d0bf005692677126136e02f60" # (2020-05-04)
repo_clone_try_double "${primary_urls[rapidcheck]}" "${secondary_urls[rapidcheck]}" "rapidcheck" "$rapidcheck_tag"

# google benchmark
benchmark_tag="v1.6.0"
repo_clone_try_double "${primary_urls[gbenchmark]}" "${secondary_urls[gbenchmark]}" "benchmark" "$benchmark_tag" true

# google test
googletest_tag="release-1.8.0"
repo_clone_try_double "${primary_urls[gtest]}" "${secondary_urls[gtest]}" "googletest" "$googletest_tag" true

# libbcrypt
libbcrypt_tag="8aa32ad94ebe06b76853b0767c910c9fbf7ccef4" # custom version (Dec 16, 2016)
repo_clone_try_double "${primary_urls[libbcrypt]}" "${secondary_urls[libbcrypt]}" "libbcrypt" "$libbcrypt_tag"

# neo4j
file_get_try_double "${primary_urls[neo4j]}" "${secondary_urls[neo4j]}"
tar -xzf neo4j-community-3.2.3-unix.tar.gz
mv neo4j-community-3.2.3 neo4j
rm neo4j-community-3.2.3-unix.tar.gz

# nlohmann json
# We wget header instead of cloning repo since repo is huge (lots of test data).
# We use head on Sep 1, 2017 instead of last release since it was long time ago.
mkdir -p json
cd json
file_get_try_double "${primary_urls[nlohmann]}" "${secondary_urls[nlohmann]}"
cd ..

rocksdb_tag="v6.14.6" # (2020-10-14)
repo_clone_try_double "${primary_urls[rocksdb]}" "${secondary_urls[rocksdb]}" "rocksdb" "$rocksdb_tag" true
pushd rocksdb
git apply ../rocksdb.patch
popd

# mgclient
mgclient_tag="v1.3.0" # (2021-09-23)
repo_clone_try_double "${primary_urls[mgclient]}" "${secondary_urls[mgclient]}" "mgclient" "$mgclient_tag" true
sed -i 's/\${CMAKE_INSTALL_LIBDIR}/lib/' mgclient/src/CMakeLists.txt

# pymgclient
pymgclient_tag="4f85c179e56302d46a1e3e2cf43509db65f062b3" # (2021-01-15)
repo_clone_try_double "${primary_urls[pymgclient]}" "${secondary_urls[pymgclient]}" "pymgclient" "$pymgclient_tag"

# mgconsole
mgconsole_tag="v1.1.0" # (2021-10-07)
repo_clone_try_double "${primary_urls[mgconsole]}" "${secondary_urls[mgconsole]}" "mgconsole" "$mgconsole_tag" true

spdlog_tag="v1.9.2" # (2021-08-12)
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
