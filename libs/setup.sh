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
wget -nv -O ${antlr_generator_filename} http://deps.memgraph.io/${antlr_generator_filename}
# git clone https://github.com/antlr/antlr4.git
antlr4_tag="aacd2a2c95816d8dc1c05814051d631bfec4cf3e" # v4.6
clone git://deps.memgraph.io/antlr4.git antlr4 $antlr4_tag

# cppitertools
# Use our fork that uses experimental/optional instead of unique_ptr in
# DerefHolder. Once we move memgraph to c++17 we can use cpp17 branch from
# original repo.
# git clone https://github.com/memgraph/cppitertools.git
cppitertools_tag="4231e0bc6fba2737b2a7a8a1576cf06186b0de6a" # experimental_optional 17 Aug 2017
clone git://deps.memgraph.io/cppitertools.git cppitertools $cppitertools_tag

# fmt
# git clone https://github.com/fmtlib/fmt.git
fmt_tag="7fa8f8fa48b0903deab5bb42e6760477173ac485" # v3.0.1
# Commit which fixes an issue when compiling with C++14 and higher.
fmt_cxx14_fix="b9aaa507fc49680d037fd84c043f747a395bce04"
clone git://deps.memgraph.io/fmt.git fmt $fmt_tag $fmt_cxx14_fix

# rapidcheck
# git clone https://github.com/emil-e/rapidcheck.git
rapidcheck_tag="853e14f0f4313a9eb3c71e24848373e7b843dfd1" # Jun 23, 2017
clone git://deps.memgraph.io/rapidcheck.git rapidcheck $rapidcheck_tag

# google benchmark
# git clone https://github.com/google/benchmark.git
benchmark_tag="4f8bfeae470950ef005327973f15b0044eceaceb" # v1.1.0
clone git://deps.memgraph.io/benchmark.git benchmark $benchmark_tag

# google test
# git clone https://github.com/google/googletest.git
googletest_tag="ec44c6c1675c25b9827aacd08c02433cccde7780" # v1.8.0
clone git://deps.memgraph.io/googletest.git googletest $googletest_tag

# google logging
# git clone https://github.com/memgraph/glog.git
glog_tag="042a21657e79784226babab8b942f7bd0949635f" # custom version (v0.3.5+)
clone git://deps.memgraph.io/glog.git glog $glog_tag

# google flags
# git clone https://github.com/memgraph/gflags.git
gflags_tag="b37ceb03a0e56c9f15ce80409438a555f8a67b7c" # custom version (May 6, 2017)
clone git://deps.memgraph.io/gflags.git gflags $gflags_tag

# neo4j
wget -nv http://deps.memgraph.io/neo4j-community-3.2.3-unix.tar.gz -O neo4j.tar.gz
tar -xzf neo4j.tar.gz
rm -rf neo4j
mv neo4j-community-3.2.3 neo4j
rm neo4j.tar.gz

# nlohmann json
# We wget header instead of cloning repo since repo is huge (lots of test data).
# We use head on Sep 1, 2017 instead of last release since it was long time ago.
mkdir -p json
cd json
# wget "https://raw.githubusercontent.com/nlohmann/json/91e003285312167ad8365f387438ea371b465a7e/src/json.hpp"
wget -nv http://deps.memgraph.io/json.hpp
cd ..

# ltalloc
# git clone https://github.com/r-lyeh/ltalloc.git
ltalloc_tag="43b51c14857111f993f277c46151fdfac91525a2" # Nov 16, 2017
clone git://deps.memgraph.io/ltalloc.git ltalloc $ltalloc_tag
