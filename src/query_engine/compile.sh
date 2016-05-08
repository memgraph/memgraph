#!/bin/bash

# TODO: create Makefile or cmake script
# cd compiled/cpu
# clang++ -std=c++1y create_return.cpp -o create_return.so -I../../../ -shared -fPIC
# cd ../..
# clang++ -std=c++1y -g -I../ -I ../lib/yaml-cpp/include main.cpp ../cypher/cypher.cpp -o engine -L ../lib/yaml-cpp/build -l yaml-cpp -l dl
# clang++ -std=c++1y -g -O2 -I../ main.cpp ../cypher/cypher.cpp -o engine.out -l dl -pthread
clang++ -std=c++1y -g -I../ main.cpp ../cypher/cypher.cpp -o engine.out -l dl -pthread
