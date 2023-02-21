#!/usr/bin/env python3

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

import argparse
import io
import json
import os
import subprocess
import tarfile
import tempfile

import requests

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".."))
BUILD_DIR = os.path.join(PROJECT_DIR, "build")
BINARY_DIR = os.path.join(BUILD_DIR, "tests/manual")
DEFAULT_BENCHMARK_DIR = os.path.join(BINARY_DIR, "query_performance_benchmark")
DATA_URL = (
    "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/query_performance/query_performance_benchmark.tar.gz"
)

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument(
    "--binary",
    type=str,
    default=os.path.join(BINARY_DIR, "query_performance"),
    help="Path to the binary to use for the benchmark.",
)
parser.add_argument(
    "--data-dir",
    type=str,
    default=tempfile.TemporaryDirectory().name,
    help="Path to directory that can be used as a data directory for ",
)
parser.add_argument(
    "--summary-path",
    type=str,
    default=os.path.join(DEFAULT_BENCHMARK_DIR, "summary.json"),
    help="Path to which file write the summary.",
)

parser.add_argument("--init-queries-file", type=str, default=os.path.join(DEFAULT_BENCHMARK_DIR, "dataset.cypher"))
parser.add_argument("--index-queries-file", type=str, default=os.path.join(DEFAULT_BENCHMARK_DIR, "indices.cypher"))
parser.add_argument("--split-file", type=str, default=os.path.join(DEFAULT_BENCHMARK_DIR, "split_file"))

parser.add_argument(
    "--benchmark-queries-files",
    type=str,
    default=",".join(
        [os.path.join(DEFAULT_BENCHMARK_DIR, file_name) for file_name in ["expand.cypher", "match_files.cypher"]]
    ),
)

args = parser.parse_args()

v2_results_path = os.path.join(DEFAULT_BENCHMARK_DIR, "v2_results.json")
v3_results_path = os.path.join(DEFAULT_BENCHMARK_DIR, "v3_results.json")


if os.path.exists(DEFAULT_BENCHMARK_DIR):
    print(f"Using cachced data from {DEFAULT_BENCHMARK_DIR}")
else:
    print(f"Downloading benchmark data to {DEFAULT_BENCHMARK_DIR}")
    r = requests.get(DATA_URL)
    assert r.ok, "Cannot download data"
    file_like_object = io.BytesIO(r.content)
    tar = tarfile.open(fileobj=file_like_object)
    tar.extractall(os.path.dirname(DEFAULT_BENCHMARK_DIR))

subprocess.run(
    [
        args.binary,
        f"--split-file={args.split_file}",
        f"--index-queries-file={args.index_queries_file}",
        f"--init-queries-file={args.init_queries_file}",
        f"--benchmark-queries-files={args.benchmark_queries_files}",
        "--use-v3=false",
        "--use-multi-frame=true",
        f"--export-json-results={v2_results_path}",
        f"--data-directory={args.data_dir}",
    ]
)

subprocess.run(
    [
        args.binary,
        f"--split-file={args.split_file}",
        f"--index-queries-file={args.index_queries_file}",
        f"--init-queries-file={args.init_queries_file}",
        f"--benchmark-queries-files={args.benchmark_queries_files}",
        "--use-v3=true",
        "--use-multi-frame=true",
        f"--export-json-results={v3_results_path}",
        f"--data-directory={args.data_dir}",
    ]
)


v2_results_file = open(v2_results_path)
v2_results = json.load(v2_results_file)
v3_results_file = open(v3_results_path)
v3_results = json.load(v3_results_file)

with open(args.summary_path, "w") as summary:
    json.dump({"v2": v2_results, "v3": v3_results}, summary)
