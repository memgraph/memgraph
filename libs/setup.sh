#!/bin/bash

# Download external dependencies.

working_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd ${working_dir}

# antlr
antlr_generator_filename="antlr-4.6-complete.jar"
wget -O ${antlr_generator_filename} http://www.antlr.org/download/${antlr_generator_filename}
git clone https://github.com/antlr/antlr4.git
antlr4_tag="aacd2a2c95816d8dc1c05814051d631bfec4cf3e" # v4.6
cd antlr4
git checkout ${antlr4_tag}
cd ..

# cppitertools
git clone https://github.com/ryanhaining/cppitertools.git
cd cppitertools
cppitertools_tag="394cc4debcd037db199551546b6fbc3ea3066722" # master 7 Oct 2016
# because last release was v0.2 and at the point when
# the lib was added master had 104 commits more than v0.2
git checkout ${cppitertools_tag}
cd ..

# fmt
git clone https://github.com/fmtlib/fmt.git
fmt_tag="7fa8f8fa48b0903deab5bb42e6760477173ac485" # v3.0.1
# Commit which fixes an issue when compiling with C++14 and higher.
fmt_cxx14_fix="b9aaa507fc49680d037fd84c043f747a395bce04"
cd fmt
git checkout ${fmt_tag}
git cherry-pick ${fmt_cxx14_fix}
cd ..

# google benchmark
git clone https://github.com/google/benchmark.git
benchmark_tag="4f8bfeae470950ef005327973f15b0044eceaceb" # v1.1.0
cd benchmark
git checkout ${benchmark_tag}
cd ..

# google test
git clone https://github.com/google/googletest.git
googletest_tag="ec44c6c1675c25b9827aacd08c02433cccde7780" # v1.8.0
cd googletest
git checkout ${googletest_tag}
cd ..

# lcov-to-coberatura-xml
git clone https://github.com/eriwen/lcov-to-cobertura-xml.git
lcov_to_xml_tag="59584761cb5da4687693faec05bf3e2b74e9dde9" # Dec 6, 2016
cd lcov-to-cobertura-xml
git checkout ${lcov_to_xml_tag}
cd ..

# google flags
git clone https://github.com/gflags/gflags.git
gflags_tag="652651b421ca5ac7b722a34a301fb656deca5198" # May 6, 2017
cd gflags
git checkout ${gflags_tag}
cd ..
