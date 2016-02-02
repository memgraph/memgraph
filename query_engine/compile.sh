#!/bin/bash

# TODO: create Makefile or cmake script
clang++ -std=c++1y -g -I../ main.cpp ../cypher/cypher.cpp -o engine
