#!/bin/bash -e

# Download external dependencies.

working_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd ${working_dir}

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
  git checkout $checkout_id
  # Apply any optional cherry pick fixes.
  while [[ $# -ne 0 ]]; do
    local cherry_pick_id=$1
    shift
    git cherry-pick -n $cherry_pick_id
  done
  # Reapply any local changes.
  if [[ $local_changes == true ]]; then
    git stash pop
  fi
  popd
}

# antlr
antlr_generator_filename="antlr-4.6-complete.jar"
# wget -O ${antlr_generator_filename} http://www.antlr.org/download/${antlr_generator_filename}
wget -nv -O ${antlr_generator_filename} https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/${antlr_generator_filename}
antlr4_tag="aacd2a2c95816d8dc1c05814051d631bfec4cf3e" # v4.6
clone https://github.com/antlr/antlr4.git antlr4 $antlr4_tag
# fix missing include
sed -i 's/^#pragma once/#pragma once\n#include <functional>/' antlr4/runtime/Cpp/runtime/src/support/CPPUtils.h
# remove shared library from install dependencies
sed -i 's/install(TARGETS antlr4_shared/install(TARGETS antlr4_shared OPTIONAL/' antlr4/runtime/Cpp/runtime/CMakeLists.txt

# cppitertools v2.0 2019-12-23
cppitertools_ref="cb3635456bdb531121b82b4d2e3afc7ae1f56d47"
clone https://github.com/ryanhaining/cppitertools.git cppitertools $cppitertools_ref

# fmt
fmt_tag="7bdf0628b1276379886c7f6dda2cef2b3b374f0b" # (2020-11-25) 
clone https://github.com/fmtlib/fmt.git fmt $fmt_tag

# rapidcheck
rapidcheck_tag="7bc7d302191a4f3d0bf005692677126136e02f60" # (2020-05-04)
clone https://github.com/emil-e/rapidcheck.git rapidcheck $rapidcheck_tag

# google benchmark
benchmark_tag="4f8bfeae470950ef005327973f15b0044eceaceb" # v1.1.0
clone https://github.com/google/benchmark.git benchmark $benchmark_tag

# google test
googletest_tag="ec44c6c1675c25b9827aacd08c02433cccde7780" # v1.8.0
clone https://github.com/google/googletest.git googletest $googletest_tag

# google flags
gflags_tag="b37ceb03a0e56c9f15ce80409438a555f8a67b7c" # custom version (May 6, 2017)
clone https://github.com/memgraph/gflags.git gflags $gflags_tag

# libbcrypt
libbcrypt_tag="8aa32ad94ebe06b76853b0767c910c9fbf7ccef4" # custom version (Dec 16, 2016)
clone https://github.com/rg3/libbcrypt libbcrypt $libbcrypt_tag

# neo4j
wget -nv https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/neo4j-community-3.2.3-unix.tar.gz -O neo4j.tar.gz
tar -xzf neo4j.tar.gz
rm -rf neo4j
mv neo4j-community-3.2.3 neo4j
rm neo4j.tar.gz

# nlohmann json
# We wget header instead of cloning repo since repo is huge (lots of test data).
# We use head on Sep 1, 2017 instead of last release since it was long time ago.
mkdir -p json
cd json
wget "https://raw.githubusercontent.com/nlohmann/json/b3e5cb7f20dcc5c806e418df34324eca60d17d4e/single_include/nlohmann/json.hpp"
cd ..

bzip2_tag="0405487e2b1de738e7f1c8afb50d19cf44e8d580"  # v1.0.6 (May 26, 2011)
clone https://github.com/VFR-maniac/bzip2 bzip2 $bzip2_tag

zlib_tag="cacf7f1d4e3d44d871b605da3b647f07d718623f" # v1.2.11.
clone https://github.com/madler/zlib.git zlib $zlib_tag
# remove shared library from install dependencies
sed -i 's/install(TARGETS zlib zlibstatic/install(TARGETS zlibstatic/g' zlib/CMakeLists.txt

rocksdb_tag="f3e33549c151f30ac4eb7c22356c6d0331f37652" # (2020-10-14)
clone https://github.com/facebook/rocksdb.git rocksdb $rocksdb_tag
# remove shared library from install dependencies
sed -i 's/TARGETS ${ROCKSDB_SHARED_LIB}/TARGETS ${ROCKSDB_SHARED_LIB} OPTIONAL/' rocksdb/CMakeLists.txt

# mgclient
mgclient_tag="v1.2.0" # (2021-01-14)
clone https://github.com/memgraph/mgclient.git mgclient $mgclient_tag
sed -i 's/\${CMAKE_INSTALL_LIBDIR}/lib/' mgclient/src/CMakeLists.txt

# pymgclient
pymgclient_tag="4f85c179e56302d46a1e3e2cf43509db65f062b3" # (2021-01-15)
clone https://github.com/memgraph/pymgclient.git pymgclient $pymgclient_tag

spdlog_tag="46d418164dd4cd9822cf8ca62a116a3f71569241" # (2020-12-01)
clone https://github.com/gabime/spdlog spdlog $spdlog_tag
