#!/bin/bash

# antlr
wget http://www.antlr.org/download/antlr-4.6-complete.jar
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
fmt_tag="e5e4fb370ccf327bbdcdcd782eb3e53580e11094" # v3.0.0
cd fmt
git checkout ${fmt_tag}
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

# yaml-cpp
git clone https://github.com/jbeder/yaml-cpp
yaml_cpp_tag="519d33fea3fbcbe7e1f89f97ee0fa539cec33eb7" # master 18 Aug 2016
# because a bug with link process had been fixed somewhen between
# this commit and v0.5.3
cd yaml-cpp
git checkout ${yaml_cpp_tag}
cd ..
