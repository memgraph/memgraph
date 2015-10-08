#!/bin/bash

cd api && python link_resources.py && cd ..
make clean && make
