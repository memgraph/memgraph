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

import collections
import copy
import fnmatch
import importlib
import inspect
import json
import os
import subprocess
import sys
from pathlib import Path

import log
import workloads
from benchmark_context import BenchmarkContext
from workloads import *

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def get_binary_path(path, base=""):
    dirpath = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".."))
    if os.path.exists(os.path.join(dirpath, "build_release")):
        dirpath = os.path.join(dirpath, "build_release")
    else:
        dirpath = os.path.join(dirpath, "build")
    return os.path.join(dirpath, path)


def download_file(url, path):
    ret = subprocess.run(["wget", "-nv", "--content-disposition", url], stderr=subprocess.PIPE, cwd=path, check=True)
    data = ret.stderr.decode("utf-8")
    tmp = data.split("->")[1]
    name = tmp[tmp.index('"') + 1 : tmp.rindex('"')]
    return os.path.join(path, name)


def unpack_gz_and_move_file(input_path, output_path):
    if input_path.endswith(".gz"):
        subprocess.run(["gunzip", input_path], stdout=subprocess.DEVNULL, check=True)
        input_path = input_path[:-3]
    os.rename(input_path, output_path)


def unpack_gz(input_path: Path):
    if input_path.suffix == ".gz":
        subprocess.run(["gzip", "-d", input_path], capture_output=True, check=True)
        input_path = input_path.with_suffix("")
    return input_path


def unpack_zip(input_path: Path):
    if input_path.suffix == ".zip":
        subprocess.run(["unzip", input_path], capture_output=True, check=True, cwd=input_path.parent)
        input_path = input_path.with_suffix("")
    return input_path


def unpack_tar_zst(input_path: Path):
    if input_path.suffix == ".zst":
        subprocess.run(
            ["tar", "--use-compress-program=unzstd", "-xvf", input_path],
            cwd=input_path.parent,
            capture_output=True,
            check=True,
        )
        input_path = input_path.with_suffix("").with_suffix("")
    return input_path


def unpack_tar_gz(input_path: Path):
    if input_path.suffix == ".gz":
        subprocess.run(
            ["tar", "-xvf", input_path],
            cwd=input_path.parent,
            capture_output=True,
            check=True,
        )
        input_path = input_path.with_suffix("").with_suffix("")
    return input_path


def unpack_tar_zst_and_move(input_path: Path, output_path: Path):
    if input_path.suffix == ".zst":
        subprocess.run(
            ["tar", "--use-compress-program=unzstd", "-xvf", input_path],
            cwd=input_path.parent,
            capture_output=True,
            check=True,
        )
        input_path = input_path.with_suffix("").with_suffix("")
    return input_path.rename(output_path)


def ensure_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)
    if not os.path.isdir(path):
        raise Exception("The path '{}' should be a directory!".format(path))


def get_available_workloads(customer_workloads: str = None) -> dict:
    generators = {}
    for module in map(workloads.__dict__.get, workloads.__all__):
        for key in dir(module):
            if key.startswith("_"):
                continue
            base_class = getattr(module, key)
            if not inspect.isclass(base_class) or not issubclass(base_class, workloads.base.Workload):
                continue
            queries = collections.defaultdict(list)
            for funcname in dir(base_class):
                if not funcname.startswith("benchmark__"):
                    continue
                group, query = funcname.split("__")[1:]
                queries[group].append((query, funcname))
            generators[base_class.NAME] = (base_class, dict(queries))

    if customer_workloads:
        head_tail = os.path.split(customer_workloads)
        path_without_dataset_name = head_tail[0]
        dataset_name = head_tail[1].split(".")[0]
        sys.path.append(path_without_dataset_name)
        dataset_to_use = importlib.import_module(dataset_name)

        for key in dir(dataset_to_use):
            if key.startswith("_"):
                continue
            base_class = getattr(dataset_to_use, key)
            if not inspect.isclass(base_class) or not issubclass(base_class, workloads.base.Workload):
                continue
            queries = collections.defaultdict(list)
            for funcname in dir(base_class):
                if not funcname.startswith("benchmark__"):
                    continue
                group, query = funcname.split("__")[1:]
                queries[group].append((query, funcname))
            generators[base_class.NAME] = (base_class, dict(queries))

    return generators


def list_available_workloads(customer_workloads: str = None):
    generators = get_available_workloads(customer_workloads)
    for name in sorted(generators.keys()):
        print("Dataset:", name)
        dataset, queries = generators[name]
        print(
            "    Variants:",
            ", ".join(dataset.VARIANTS),
            "(default: " + dataset.DEFAULT_VARIANT + ")",
        )
        for group in sorted(queries.keys()):
            print("    Group:", group)
            for query_name, query_func in queries[group]:
                print("        Query:", query_name)


def match_patterns(workload, variant, group, query, is_default_variant, patterns):
    for pattern in patterns:
        verdict = [fnmatch.fnmatchcase(workload, pattern[0])]
        if pattern[1] != "":
            verdict.append(fnmatch.fnmatchcase(variant, pattern[1]))
        else:
            verdict.append(is_default_variant)
        verdict.append(fnmatch.fnmatchcase(group, pattern[2]))
        verdict.append(fnmatch.fnmatchcase(query, pattern[3]))
        if all(verdict):
            return True
    return False


def filter_workloads(available_workloads: dict, benchmark_context: BenchmarkContext) -> list:
    patterns = benchmark_context.benchmark_target_workload
    for i in range(len(patterns)):
        pattern = patterns[i].split("/")
        if len(pattern) > 5 or len(pattern) == 0:
            raise Exception("Invalid benchmark description '" + pattern + "'!")
        pattern.extend(["", "*", "*"][len(pattern) - 1 :])
        patterns[i] = pattern

    filtered = []
    for workload in sorted(available_workloads.keys()):
        generator, queries = available_workloads[workload]
        for variant in generator.VARIANTS:
            is_default_variant = variant == generator.DEFAULT_VARIANT
            current = collections.defaultdict(list)
            for group in queries:
                for query_name, query_func in queries[group]:
                    if match_patterns(
                        workload,
                        variant,
                        group,
                        query_name,
                        is_default_variant,
                        patterns,
                    ):
                        current[group].append((query_name, query_func))
            if len(current) == 0:
                continue

            # Ignore benchgraph "basic" queries in standard CI/CD run
            for pattern in patterns:
                res = pattern.count("*")
                key = "basic"
                if res >= 2 and key in current.keys():
                    current.pop(key)

            filtered.append((generator(variant=variant, benchmark_context=benchmark_context), dict(current)))
    return filtered


def parse_kwargs(items):
    """
    Parse a series of key-value pairs and return a dictionary
    """
    d = {}

    if items:
        for item in items:
            key, value = item.split("=")
            d[key] = value
    return d


class Directory:
    def __init__(self, path):
        self._path = path

    def get_path(self):
        return self._path

    def get_file(self, name):
        path = os.path.join(self._path, name)
        if os.path.exists(path) and not os.path.isfile(path):
            raise Exception("The path '{}' should be a file!".format(path))
        return (path, os.path.isfile(path))


class RecursiveDict:
    def __init__(self, data={}):
        self._data = copy.deepcopy(data)

    def _get_obj_and_key(self, *args):
        key = args[-1]
        obj = self._data
        for item in args[:-1]:
            if item not in obj:
                obj[item] = {}
            obj = obj[item]
        return (obj, key)

    def get_value(self, *args):
        obj, key = self._get_obj_and_key(*args)
        return obj.get(key, None)

    def set_value(self, *args, value=None):
        obj, key = self._get_obj_and_key(*args)
        obj[key] = value

    def get_data(self):
        return copy.deepcopy(self._data)


class Cache:
    def __init__(self):
        self._directory = os.path.join(SCRIPT_DIR, ".cache")
        ensure_directory(self._directory)
        self._config = os.path.join(self._directory, "config.json")

    def cache_directory(self, *args):
        if len(args) == 0:
            raise ValueError("At least one directory level must be supplied!")
        path = os.path.join(self._directory, *args)
        ensure_directory(path)
        return Directory(path)

    def get_default_cache_directory(self):
        return self._directory

    def load_config(self):
        if not os.path.isfile(self._config):
            return RecursiveDict()
        with open(self._config) as f:
            return RecursiveDict(json.load(f))

    def save_config(self, config):
        with open(self._config, "w") as f:
            json.dump(config.get_data(), f)
