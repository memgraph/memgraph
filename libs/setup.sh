#!/bin/bash -e

# Download external dependencies.

working_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd ${working_dir}

# Cleanup existing dependencies.
./cleanup.sh

# antlr
antlr_generator_filename="antlr-4.6-complete.jar"
# wget -O ${antlr_generator_filename} http://www.antlr.org/download/${antlr_generator_filename}
wget -nv -O ${antlr_generator_filename} http://deps.memgraph.io/${antlr_generator_filename}
# git clone https://github.com/antlr/antlr4.git
git clone git://deps.memgraph.io/antlr4.git
antlr4_tag="aacd2a2c95816d8dc1c05814051d631bfec4cf3e" # v4.6
cd antlr4
git checkout ${antlr4_tag}
cd ..

# cppitertools
# Use our fork that uses experimental/optional instead of unique_ptr in
# DerefHolder. Once we move memgraph to c++17 we can use cpp17 branch from
# original repo.
# git clone https://github.com/memgraph/cppitertools.git
git clone git://deps.memgraph.io/cppitertools.git
cd cppitertools
cppitertools_tag="4231e0bc6fba2737b2a7a8a1576cf06186b0de6a" # experimental_optional 17 Aug 2017
git checkout ${cppitertools_tag}
cd ..

# fmt
# git clone https://github.com/fmtlib/fmt.git
git clone git://deps.memgraph.io/fmt.git
fmt_tag="7fa8f8fa48b0903deab5bb42e6760477173ac485" # v3.0.1
# Commit which fixes an issue when compiling with C++14 and higher.
fmt_cxx14_fix="b9aaa507fc49680d037fd84c043f747a395bce04"
cd fmt
git checkout ${fmt_tag}
git cherry-pick -n ${fmt_cxx14_fix}
cd ..

# rapidcheck
# git clone https://github.com/emil-e/rapidcheck.git
git clone git://deps.memgraph.io/rapidcheck.git
rapidcheck_tag="853e14f0f4313a9eb3c71e24848373e7b843dfd1" # Jun 23, 2017
cd rapidcheck
git checkout ${rapidcheck_tag}
cd ..

# google benchmark
# git clone https://github.com/google/benchmark.git
git clone git://deps.memgraph.io/benchmark.git
benchmark_tag="4f8bfeae470950ef005327973f15b0044eceaceb" # v1.1.0
cd benchmark
git checkout ${benchmark_tag}
cd ..

# google test
# git clone https://github.com/google/googletest.git
git clone git://deps.memgraph.io/googletest.git
googletest_tag="ec44c6c1675c25b9827aacd08c02433cccde7780" # v1.8.0
cd googletest
git checkout ${googletest_tag}
cd ..

# google logging
# git clone https://github.com/memgraph/glog.git
git clone git://deps.memgraph.io/glog.git
glog_tag="042a21657e79784226babab8b942f7bd0949635f" # custom version (v0.3.5+)
cd glog
git checkout ${glog_tag}
cd ..

# lcov-to-coberatura-xml
# git clone https://github.com/eriwen/lcov-to-cobertura-xml.git
git clone git://deps.memgraph.io/lcov-to-cobertura-xml.git
lcov_to_xml_tag="59584761cb5da4687693faec05bf3e2b74e9dde9" # Dec 6, 2016
cd lcov-to-cobertura-xml
git checkout ${lcov_to_xml_tag}
cd ..

# google flags
# git clone https://github.com/memgraph/gflags.git
git clone git://deps.memgraph.io/gflags.git
gflags_tag="b37ceb03a0e56c9f15ce80409438a555f8a67b7c" # custom version (May 6, 2017)
cd gflags
git checkout ${gflags_tag}
cd ..

# neo4j
wget -nv http://deps.memgraph.io/neo4j-community-3.2.3-unix.tar.gz -O neo4j.tar.gz
tar -xzf neo4j.tar.gz
mv neo4j-community-3.2.3 neo4j
rm neo4j.tar.gz

# postgresql
wget -nv http://deps.memgraph.io/postgresql-9.6.5-1-linux-x64-binaries.tar.gz -O postgres.tar.gz
tar -xzf postgres.tar.gz
mv pgsql postgresql
rm postgres.tar.gz

# nlohmann json
# We wget header instead of cloning repo since repo is huge (lots of test data).
# We use head on Sep 1, 2017 instead of last release since it was long time ago.
mkdir json
cd json
# wget "https://raw.githubusercontent.com/nlohmann/json/91e003285312167ad8365f387438ea371b465a7e/src/json.hpp"
wget -nv http://deps.memgraph.io/json.hpp
cd ..

# ltalloc
# git clone https://github.com/r-lyeh/ltalloc.git
git clone git://deps.memgraph.io/ltalloc.git
ltalloc_tag="43b51c14857111f993f277c46151fdfac91525a2" # Nov 16, 2017
cd ltalloc
git checkout ${ltalloc_tag}
cd ..

# cereal
#git clone https://github.com/USCiLab/cereal.git
git clone git://deps.memgraph.io/cereal.git
cd cereal
git checkout v1.2.2
cd ..
