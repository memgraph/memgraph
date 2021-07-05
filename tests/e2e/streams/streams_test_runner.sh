#!/bin/sh

# This workaround is necessary to run in the same virtualenv as the e2e runner.py

BASEDIR=$(dirname ${BASH_SOURCE[0]})
python3 $BASEDIR/streams_tests.py