# Copyright 2023 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import zipfile
from pathlib import Path

import log


def zip_benchgraph():
    log.info("Creating benchgraph.zip ...")
    parent = Path(__file__).resolve().parent
    zip = zipfile.ZipFile("./benchgraph.zip", "w")
    zip.write(parent / "benchmark.py", "benchgraph/benchmark.py")
    zip.write(parent / "setup.py", "benchgraph/setup.py")
    zip.write(parent / "log.py", "benchgraph/log.py")
    zip.write(parent / "benchmark_context.py", "benchgraph/benchmark_context.py")
    zip.write(parent / "validation.py", "benchgraph/validation.py")
    zip.write(parent / "compare_results.py", "benchgraph/compare_results.py")
    zip.write(parent / "runners.py", "benchgraph/runners.py")
    zip.write(parent / "helpers.py", "benchgraph/helpers.py")
    zip.write(parent / "graph_bench.py", "benchgraph/graph_bench.py")
    zip.write(parent / "README.md", "benchgraph/README.md")
    zip.write(parent / "how_to_use_benchgraph.md", "benchgraph/how_to_use_benchgraph.md")
    zip.write(parent / "workloads/__init__.py", "benchgraph/workloads/__init__.py")
    zip.write(parent / "workloads/base.py", "benchgraph/workloads/base.py")
    zip.write(parent / "workloads/demo.py", "benchgraph/workloads/demo.py")

    zip.close()


if __name__ == "__main__":
    zip_benchgraph()

    if Path("./benchgraph.zip").is_file():
        log.success("benchgraph.zip created successfully")
    else:
        log.error("benchgraph.zip was not created")
