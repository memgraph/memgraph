#!/usr/bin/env python3
import json
import os
import re
import subprocess

# paths
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
TESTS_DIR_REL = os.path.join("..", "..", "build", "tools", "tests")
TESTS_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, TESTS_DIR_REL))

# ctest tests
ctest_output = subprocess.run(["ctest", "-N"], cwd=TESTS_DIR, check=True,
        stdout=subprocess.PIPE).stdout.decode("utf-8")

runs = []

# test ordering: first unit, then concurrent, then everything else
for row in ctest_output.split("\n"):
    # Filter rows only containing tests.
    if not re.match("^\s*Test\s+#", row): continue
    name = row.split(":")[1].strip()
    path = os.path.join(TESTS_DIR_REL, name)

    dirname, basename = os.path.split(path)
    files = [basename, "CTestTestfile.cmake"]

    # extra files for specific tests
    if name == "test_mg_import_csv":
        files.extend(["csv", "mg_recovery_check", "../src/mg_import_csv"])

    runs.append({
        "name": "tools__" + name,
        "cd": dirname,
        "commands": "ctest --output-on-failure -R \"^{}$\"".format(name),
        "infiles": files,
    })

print(json.dumps(runs, indent=4, sort_keys=True))
