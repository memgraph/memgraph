#!/bin/bash -e

# Download external dependencies.

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
  shift 3
  # Clone if there's no repo.
  if [[ ! -d "$dir_name" ]]; then
    git clone "$git_repo" "$dir_name"
  fi
  pushd "$dir_name"
  # Just fetch new commits from remote repository. Don't merge/pull them in, so
  # that we don't clobber local modifications.
  git fetch
  # Check whether we have any local changes which need to be preserved.
  local local_changes=true
  if git diff --no-ext-diff --quiet && git diff --no-ext-diff --cached --quiet; then
    local_changes=false
  fi
  # Stash regardless of local_changes, so that a user gets a message on stdout.
  git stash
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
    echo "Cloning primary from $primary_url secondary from $secondary_url"
    if [ -z "$primary_url" ]; then echo "Primary should not be empty." && exit 1; fi
    if [ -z "$secondary_url" ]; then echo "Secondary should not be empty." && exit 1; fi
    if [ -z "$folder_name" ]; then echo "Clone folder should not be empty." && exit 1; fi
    if [ -z "$ref" ]; then echo "Git clone ref should not be empty." && exit 1; fi
    clone "$primary_url" "$folder_name" "$ref" || clone "$secondary_url" "$folder_name" "$ref" || exit 1
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
  ["antlr4-generator"]="http://$local_cache_host/file/antlr-4.6-complete.jar"
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
  ["spdlog"]="http://$local_cache_host/git/spdlog"
  ["jemalloc"]="http://$local_cache_host/git/jemalloc.git"
  ["nlohmann"]="http://$local_cache_host/file/nlohmann/json/b3e5cb7f20dcc5c806e418df34324eca60d17d4e/single_include/nlohmann/json.hpp"
  ["neo4j"]="http://$local_cache_host/file/neo4j-community-3.2.3-unix.tar.gz"
)

# The goal of secondary urls is to have links to the "source of truth" of
# dependencies, e.g., Github or S3. Download from secondary urls, if happens
# at all, should never fail. In other words, if it fails, the whole build
# should fail.
declare -A secondary_urls=(
  ["antlr4-code"]="https://github.com/antlr/antlr4.git"
  ["antlr4-generator"]="http://www.antlr.org/download/antlr-4.6-complete.jar"
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
  ["spdlog"]="https://github.com/gabime/spdlog"
  ["jemalloc"]="https://github.com/jemalloc/jemalloc.git"
  ["nlohmann"]="https://raw.githubusercontent.com/nlohmann/json/b3e5cb7f20dcc5c806e418df34324eca60d17d4e/single_include/nlohmann/json.hpp"
  ["neo4j"]="https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/neo4j-community-3.2.3-unix.tar.gz"
)

# antlr
file_get_try_double "${primary_urls[antlr4-generator]}" "${secondary_urls[antlr4-generator]}"

antlr4_tag="aacd2a2c95816d8dc1c05814051d631bfec4cf3e" # v4.6
repo_clone_try_double "${primary_urls[antlr4-code]}" "${secondary_urls[antlr4-code]}" "antlr4" "$antlr4_tag"
# fix missing include
sed -i 's/^#pragma once/#pragma once\n#include <functional>/' antlr4/runtime/Cpp/runtime/src/support/CPPUtils.h
# remove shared library from install dependencies
sed -i 's/install(TARGETS antlr4_shared/install(TARGETS antlr4_shared OPTIONAL/' antlr4/runtime/Cpp/runtime/CMakeLists.txt

# cppitertools v2.0 2019-12-23
cppitertools_ref="cb3635456bdb531121b82b4d2e3afc7ae1f56d47"
repo_clone_try_double "${primary_urls[cppitertools]}" "${secondary_urls[cppitertools]}" "cppitertools" "$cppitertools_ref"

# fmt
fmt_tag="7bdf0628b1276379886c7f6dda2cef2b3b374f0b" # (2020-11-25)
repo_clone_try_double "${primary_urls[fmt]}" "${secondary_urls[fmt]}" "fmt" "$fmt_tag"

# rapidcheck
rapidcheck_tag="7bc7d302191a4f3d0bf005692677126136e02f60" # (2020-05-04)
repo_clone_try_double "${primary_urls[rapidcheck]}" "${secondary_urls[rapidcheck]}" "rapidcheck" "$rapidcheck_tag"

# google benchmark
benchmark_tag="4f8bfeae470950ef005327973f15b0044eceaceb" # v1.1.0
repo_clone_try_double "${primary_urls[gbenchmark]}" "${secondary_urls[gbenchmark]}" "benchmark" "$benchmark_tag"

# google test
googletest_tag="ec44c6c1675c25b9827aacd08c02433cccde7780" # v1.8.0
repo_clone_try_double "${primary_urls[gtest]}" "${secondary_urls[gtest]}" "googletest" "$googletest_tag"

# google flags
gflags_tag="b37ceb03a0e56c9f15ce80409438a555f8a67b7c" # custom version (May 6, 2017)
repo_clone_try_double "${primary_urls[gflags]}" "${secondary_urls[gflags]}" "gflags" "$gflags_tag"

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

bzip2_tag="0405487e2b1de738e7f1c8afb50d19cf44e8d580"  # v1.0.6 (May 26, 2011)
repo_clone_try_double "${primary_urls[bzip2]}" "${secondary_urls[bzip2]}" "bzip2" "$bzip2_tag"

zlib_tag="cacf7f1d4e3d44d871b605da3b647f07d718623f" # v1.2.11.
repo_clone_try_double "${primary_urls[zlib]}" "${secondary_urls[zlib]}" "zlib" "$zlib_tag"
# remove shared library from install dependencies
sed -i 's/install(TARGETS zlib zlibstatic/install(TARGETS zlibstatic/g' zlib/CMakeLists.txt

rocksdb_tag="f3e33549c151f30ac4eb7c22356c6d0331f37652" # (2020-10-14)
repo_clone_try_double "${primary_urls[rocksdb]}" "${secondary_urls[rocksdb]}" "rocksdb" "$rocksdb_tag"
# remove shared library from install dependencies
sed -i 's/TARGETS ${ROCKSDB_SHARED_LIB}/TARGETS ${ROCKSDB_SHARED_LIB} OPTIONAL/' rocksdb/CMakeLists.txt

# mgclient
mgclient_tag="v1.2.0" # (2021-01-14)
repo_clone_try_double "${primary_urls[mgclient]}" "${secondary_urls[mgclient]}" "mgclient" "$mgclient_tag"
sed -i 's/\${CMAKE_INSTALL_LIBDIR}/lib/' mgclient/src/CMakeLists.txt

# pymgclient
pymgclient_tag="4f85c179e56302d46a1e3e2cf43509db65f062b3" # (2021-01-15)
repo_clone_try_double "${primary_urls[pymgclient]}" "${secondary_urls[pymgclient]}" "pymgclient" "$pymgclient_tag"

spdlog_tag="46d418164dd4cd9822cf8ca62a116a3f71569241" # (2020-12-01)
repo_clone_try_double "${primary_urls[spdlog]}" "${secondary_urls[spdlog]}" "spdlog" "$spdlog_tag"

jemalloc_tag="ea6b3e973b477b8061e0076bb257dbd7f3faa756" # (2021-02-11)
repo_clone_try_double "${primary_urls[jemalloc]}" "${secondary_urls[jemalloc]}" "jemalloc" "$jemalloc_tag"
pushd jemalloc
# ThreadPool select job randomly, and there can be some threads that had been
# performed some memory heavy task before and will be inactive for some time,
# but until it will became active again, the memory will not be freed since by
# default each thread has it's own arena, but there should be not more then
# 4*CPU arenas (see opt.nareans description).
#
# By enabling percpu_arena number of arenas limited to number of CPUs and hence
# this problem should go away.
#
# muzzy_decay_ms -- use MADV_FREE when available on newer Linuxes, to
# avoid spurious latencies and additional work associated with
# MADV_DONTNEED. See
# https://github.com/ClickHouse/ClickHouse/issues/11121 for motivation.
./autogen.sh --with-malloc-conf="percpu_arena:percpu,oversize_threshold:0,muzzy_decay_ms:10000"
popd
