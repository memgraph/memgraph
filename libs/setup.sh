#!/bin/bash

# Catch
git clone https://github.com/philsquared/Catch.git
catch_tag="master"
cd Catch
git checkout ${catch_tag}
cd ..

# fmt
git clone https://github.com/fmtlib/fmt.git
fmt_tag="e5e4fb370ccf327bbdcdcd782eb3e53580e11094"
cd fmt
git checkout ${fmt_tag}
cmake .
make
cd ..

# yaml-cpp
git clone https://github.com/jbeder/yaml-cpp
yaml_cpp_tag="519d33fea3fbcbe7e1f89f97ee0fa539cec33eb7"
cd yaml-cpp
git checkout ${yaml_cpp_tag}
cmake .
make
cd ..

# lemon
mkdir lemon
cd lemon
lemon_tag="09a96bed19955697a5e20c49ad863ec2005815a2"
wget http://www.sqlite.org/src/raw/tool/lemon.c?name=${lemon_tag} -O lemon.c
lempar_tag="8c4e9d8517e50da391f1d89a519e743dd4afbc09"
wget http://www.sqlite.org/src/raw/tool/lempar.c?name=${lempar_tag} -O lempar.c
clang lemon.c -o lemon -O2
cd ..

# lexertl
git clone https://github.com/BenHanson/lexertl.git
lexertl_tag=7d4d36a357027df0e817453cc9cf948f71047ca9
cd lexertl
git checkout ${lexertl_tag}
cd ..
