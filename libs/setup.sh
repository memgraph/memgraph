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

# http_parser
git clone https://github.com/nodejs/http-parser
http_parser_tag="4e382f96e6d3321538a78f2c7f9506d4e79b08d6"
cd http-parser
git checkout ${http_parser_tag}
make package
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

# libuv
git clone https://github.com/libuv/libuv.git
libuv_tag="c82eedd0a76233f0894098853b5a0c307af27064"
cd libuv
git checkout ${libuv_tag}
./autogen.sh && ./configure && make
cd ..

# rapidjson
git clone https://github.com/miloyip/rapidjson.git
rapidjson_tag=${3d5848a7cd3367c5cb451c6493165b7745948308}
cd rapidjson
git checkout ${rapidjson_tag}
cd ..

# r3

# UBUNTU 16.04
# TODO: automate this
# sudo apt-get -y install check libpcre3 libpcre3-dev libjemalloc-dev
# sudo apt-get -y install libjemalloc1 build-essential libtool automake
# sudo apt-get -y install autoconf pkg-config

git clone https://github.com/c9s/r3.git
r3_tag=28726b27af3cd0a9d3166033c6619a9c7227cb48
cd r3
git checkout ${r3_tag}
./autogen.sh && ./configure && make
cd ..
