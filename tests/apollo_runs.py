#!/usr/bin/env python3
import json
import os
import re
import subprocess

# paths
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
WORKSPACE_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".."))
TESTS_DIR_REL = os.path.join("..", "build_debug", "tests")
TESTS_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, TESTS_DIR_REL))

# test ordering: first unit, then concurrent, then everything else
CTEST_ORDER = {"unit": 0, "concurrent": 1}
CTEST_DELIMITER = "__"


def get_runs(build_dir, include=None, exclude=None, outfile=None,
             name_prefix=""):
    tests_dir = os.path.join(build_dir, "tests")
    tests_dir_abs = os.path.normpath(os.path.join(SCRIPT_DIR, tests_dir))
    ctest_output = subprocess.run(
        ["ctest", "-N"], cwd=tests_dir_abs, check=True,
        stdout=subprocess.PIPE).stdout.decode("utf-8")
    tests = []

    for row in ctest_output.split("\n"):
        # Filter rows only containing tests.
        if not re.match("^\s*Test\s+#", row):
            continue
        if not row.count("memgraph"):
            continue
        test_name = row.split(":")[1].strip()
        name = test_name.replace("memgraph" + CTEST_DELIMITER, "")
        path = os.path.join(
            tests_dir, name.replace(CTEST_DELIMITER, "/", 1))
        order = CTEST_ORDER.get(
            name.split(CTEST_DELIMITER)[0], len(CTEST_ORDER))
        tests.append((order, name, path))

    tests.sort()

    runs = []
    for test in tests:
        order, name, path = test
        dirname, basename = os.path.split(path)
        files = [basename]

        # check whether the test should be included
        if include is not None:
            should_include = False
            for inc in include:
                if name.startswith(inc):
                    should_include = True
                    break
            if not should_include:
                continue

        # check whether the test should be excluded
        if exclude is not None:
            should_exclude = False
            for exc in exclude:
                if name.startswith(exc):
                    should_exclude = True
                    break
            if should_exclude:
                continue

        # larger timeout for benchmark and concurrent tests
        prefix = ""
        if name.startswith("benchmark") or name.startswith("concurrent"):
            prefix = "TIMEOUT=600 "

        # larger timeout for storage_v2_durability unit test
        if name.endswith("storage_v2_durability"):
            prefix = "TIMEOUT=300 "

        # py_module unit test requires user-facing 'mgp' module
        if name.endswith("py_module"):
            mgp_path = os.path.join("..", "include", "mgp.py")
            files.append(os.path.relpath(mgp_path, dirname))

        # get output files
        outfile_paths = []
        if outfile:
            curdir_abs = os.path.normpath(os.path.join(SCRIPT_DIR, dirname))
            curdir_rel = os.path.relpath(curdir_abs, WORKSPACE_DIR)
            outfile_paths.append("\./" + curdir_rel.replace(".", "\\.") + "/" +
                                 outfile.replace(".", "\\."))

        runs.append({
            "name": name_prefix + name,
            "cd": dirname,
            "commands": prefix + "./" + basename,
            "infiles": files,
            "outfile_paths": outfile_paths,
        })
    return runs


# generation mode
mode = "release"
if os.environ.get("PROJECT", "") == "mg-master-diff":
    mode = "diff"

# get unit tests
runs = get_runs("../build_coverage", include=["unit"],
                outfile="default.profraw")

# get all other tests except unit and benchmark
runs += get_runs("../build_debug", exclude=["unit", "benchmark"])

# get benchmark tests
if mode != "diff":
    runs += get_runs("../build_release", include=["benchmark"])

# get community tests
runs += get_runs("../build_community", include=["unit"],
                 name_prefix="community__")

print(json.dumps(runs, indent=4, sort_keys=True))
