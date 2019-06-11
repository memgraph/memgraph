#!/usr/bin/env python3
import json
import os
import re
import subprocess

from card_fraud import NUM_MACHINES, BINARIES

# paths
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
WORKSPACE_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", "..", ".."))
OUTPUT_DIR_REL = os.path.join(os.path.relpath(SCRIPT_DIR, WORKSPACE_DIR), "output")

# generate runs
runs = []

binaries = list(map(lambda x: os.path.join("..", "..", "build_release", x), BINARIES))

for i in range(NUM_MACHINES):
    name = "master" if i == 0 else "worker" + str(i)
    additional = ["master.py"] if i == 0 else []
    outfile_paths = ["\\./" + OUTPUT_DIR_REL + "/.+"] if i == 0 else []
    if i == 0:
        cmd = "master.py"
        args = "--machines-num {0} --test-suite card_fraud " \
                "--test card_fraud".format(NUM_MACHINES)
    else:
        cmd = "jail_service.py"
        args = ""
    runs.append({
        "name": "distributed__card_fraud__" + name,
        "cd": "..",
        "supervisor": cmd,
        "arguments": args,
        "infiles": binaries + [
            "common.py",
            "jail_service.py",
            "card_fraud/card_fraud.py",
            "card_fraud/snapshots/worker_" + str(i),
        ] + additional,
        "outfile_paths": outfile_paths,
        "parallel_run": "distributed__card_fraud",
        "slave_group": "remote_4c32g",
        "enable_network": True,
    })

#print(json.dumps(runs, indent=4, sort_keys=True))
print("[]")
