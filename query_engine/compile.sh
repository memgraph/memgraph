#!/bin/bash

# TODO: create Makefile or cmake script
clang++ -std=c++1y -I../ main.cpp ../cypher/cypher.cpp -o engine
