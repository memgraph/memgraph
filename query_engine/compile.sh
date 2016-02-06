#!/bin/bash

# TODO: create Makefile or cmake script
cd compiled/cpu
clang++ -std=c++1y create_return.cpp -o create_return.so -I../../../ -shared -fPIC
cd ../..
clang++ -std=c++1y -g -I../ main.cpp ../cypher/cypher.cpp -o engine -ldl
