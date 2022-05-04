# Copyright 2021 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import os

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".."))
BUILD_DIR = os.path.join(PROJECT_DIR, "build")
MEMGRAPH_BINARY = os.path.join(BUILD_DIR, "memgraph")


def extract_bolt_port(args):
    for arg_index, arg in enumerate(args):
        if arg.startswith("--bolt-port="):
            maybe_port = arg.split("=")[1]
            if not maybe_port.isdigit():
                raise Exception("Unable to read Bolt port after --bolt-port=.")
            return int(maybe_port)
        elif arg == "--bolt-port":
            maybe_port = args[arg_index + 1]
            if not maybe_port.isdigit():
                raise Exception("Unable to read Bolt port after --bolt-port.")
            return int(maybe_port)
    return 7687


def replace_paths(path):
    return path.replace("$PROJECT_DIR", PROJECT_DIR).replace("$SCRIPT_DIR", SCRIPT_DIR).replace("$BUILD_DIR", BUILD_DIR)
