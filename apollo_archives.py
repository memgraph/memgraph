#!/usr/bin/env python3
import json
import os
import re
import subprocess
import sys

# paths
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
BUILD_OUTPUT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "build_release", "output"))

# helpers
def run_cmd(cmd, cwd):
    return subprocess.run(cmd, cwd=cwd, check=True,
        stdout=subprocess.PIPE).stdout.decode("utf-8")

# check project
if re.search(r"release", os.environ.get("PROJECT", "")) is None:
    print(json.dumps([]))
    sys.exit(0)

# generate archive
deb_name = run_cmd(["find", ".", "-maxdepth", "1", "-type", "f",
        "-name", "memgraph*.deb"], BUILD_OUTPUT_DIR).split("\n")[0][2:]
arch = run_cmd(["dpkg", "--print-architecture"], BUILD_OUTPUT_DIR).split("\n")[0]
version = deb_name.split("-")[1]
# generate Debian package file name as expected by Debian Policy
standard_deb_name = "memgraph_{}-1_{}.deb".format(version, arch)

archive_path = os.path.relpath(os.path.join(BUILD_OUTPUT_DIR,
        deb_name), SCRIPT_DIR)

archives = [{
    "name": "Release (deb package)",
    "archive": archive_path,
    "filename": standard_deb_name,
}]

print(json.dumps(archives, indent=4, sort_keys=True))
