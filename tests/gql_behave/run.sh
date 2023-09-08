#!/bin/bash

source ve3/bin/activate
pip install pymgclient
deactivate
./continuous_integration
