#!/usr/bin/env python3
import json
import os

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
BUILD_OUTPUT_DIR = os.path.join("build_release", "output")

archives = []
output_dir = os.path.join(SCRIPT_DIR, BUILD_OUTPUT_DIR)
if os.path.exists(output_dir):
    for fname in os.listdir(output_dir):
        if fname.startswith("memgraph") and fname.endswith(".deb"):
            path = os.path.join(BUILD_OUTPUT_DIR, fname)
            archives = [{
                "name": "Release " + fname.split("_")[1] + " (deb package)",
                "archive": path,
            }]

print(json.dumps(archives, indent=4, sort_keys=True))
