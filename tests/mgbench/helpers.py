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
import inspect
import json
import os
import subprocess
import sys
from pathlib import Path

from workload import ldbc

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


def generate_workload(name: str):
    generators = {}
    # (TODO) fix hardcoded ldbc
    test = dir(ldbc)

    for key in dir(ldbc):
        if key.startswith("_"):
            continue
        dataset = getattr(ldbc, key)
        if not inspect.isclass(dataset) or dataset == dataset.Dataset or not issubclass(dataset, dataset.Dataset):
            continue
        queries = collections.defaultdict(list)
        for funcname in dir(dataset):
            if not funcname.startswith("benchmark__"):
                continue
            group, query = funcname.split("__")[1:]
            queries[group].append((query, funcname))
        generators[dataset.NAME] = (dataset, dict(queries))
        # (TODO) Fix properties on edges.
        if dataset.PROPERTIES_ON_EDGES and False:
            raise Exception(
                'The "{}" dataset requires properties on edges, ' "but you have disabled them!".format(dataset.NAME)
            )
    return generators


def list_possible_workloads():
    generators = generate_workload("ldbc")
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


def filter_benchmarks(generators, patterns):
    patterns = copy.deepcopy(patterns)
    for i in range(len(patterns)):
        pattern = patterns[i].split("/")
        if len(pattern) > 5 or len(pattern) == 0:
            raise Exception("Invalid benchmark description '" + pattern + "'!")
        pattern.extend(["", "*", "*"][len(pattern) - 1 :])
        patterns[i] = pattern
    filtered = []
    for dataset in sorted(generators.keys()):
        generator, queries = generators[dataset]
        for variant in generator.VARIANTS:
            is_default_variant = variant == generator.DEFAULT_VARIANT
            current = collections.defaultdict(list)
            for group in queries:
                for query_name, query_func in queries[group]:
                    if match_patterns(
                        dataset,
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
            # (TODO) Fix vendor name.
            filtered.append((generator(variant, "memgraph"), dict(current)))
    return filtered


def match_patterns(dataset, variant, group, query, is_default_variant, patterns):
    for pattern in patterns:
        verdict = [fnmatch.fnmatchcase(dataset, pattern[0])]
        if pattern[1] != "":
            verdict.append(fnmatch.fnmatchcase(variant, pattern[1]))
        else:
            verdict.append(is_default_variant)
        verdict.append(fnmatch.fnmatchcase(group, pattern[2]))
        verdict.append(fnmatch.fnmatchcase(query, pattern[3]))
        if all(verdict):
            return True
    return False


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

    def load_config(self):
        if not os.path.isfile(self._config):
            return RecursiveDict()
        with open(self._config) as f:
            return RecursiveDict(json.load(f))

    def save_config(self, config):
        with open(self._config, "w") as f:
            json.dump(config.get_data(), f)
