#!/usr/bin/env python3
import json
import os

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def find_packages(build_output_dir):
    ret = []
    output_dir = os.path.join(SCRIPT_DIR, build_output_dir)
    if os.path.exists(output_dir):
        for fname in os.listdir(output_dir):
            if fname.startswith("memgraph") and fname.endswith(".deb"):
                path = os.path.join(build_output_dir, fname)
                ret.append({
                    "name": "Release " + fname.split("_")[1] +
                            " (deb package)",
                    "archive": path,
                })
    return ret


archives = []
# Find enterprise package(s).
archives += find_packages(os.path.join("build_release", "output"))
# Find community package(s).
archives += find_packages(os.path.join("build_community", "output"))

print(json.dumps(archives, indent=4, sort_keys=True))
