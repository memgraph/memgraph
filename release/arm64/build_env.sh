#!/bin/bash

cp -r ../../environment env_folder
docker build -f build_env.dockerfile --build-arg env_folder=env_folder --build-arg toolchain_version=toolchain-v4 -t mg_build_env .
rm -rf env_folder
