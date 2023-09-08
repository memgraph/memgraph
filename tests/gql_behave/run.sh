#!/bin/bash
# TODO(gitbuda): Setup mgclient and pymgclient properly.
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:../../libs/mgclient/lib

./continuous_integration
