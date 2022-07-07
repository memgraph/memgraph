# Copyright 2021 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import copy
import json
import os
import subprocess


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


def get_binary_path(path, base=""):
    dirpath = os.path.normpath(os.path.join(SCRIPT_DIR, "..", ".."))
    if os.path.exists(os.path.join(dirpath, "build_release")):
        dirpath = os.path.join(dirpath, "build_release")
    else:
        dirpath = os.path.join(dirpath, "build")
    return os.path.join(dirpath, path)


def download_file(url, path):
    ret = subprocess.run(
        ["wget", "-nv", "--content-disposition", url],
        stderr=subprocess.PIPE,
        cwd=path,
        check=True,
    )
    data = ret.stderr.decode("utf-8")
    tmp = data.split("->")[1]
    name = tmp[tmp.index('"') + 1 : tmp.rindex('"')]
    return os.path.join(path, name)


def unpack_and_move_file(input_path, output_path):
    if input_path.endswith(".gz"):
        subprocess.run(["gunzip", input_path], stdout=subprocess.DEVNULL, check=True)
        input_path = input_path[:-3]
    os.rename(input_path, output_path)


def ensure_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)
    if not os.path.isdir(path):
        raise Exception("The path '{}' should be a directory!".format(path))


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
