#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

import os
import re
from pathlib import Path


def config_to_dict(configs):
    out = dict()
    for res in configs:
        assert len(res) == 4, "Configuration results should contain 4 elements (name, default, current, description)"
        out[res[0]] = res[2]
    return out


def get_configs(session):
    return config_to_dict(session.run("SHOW CONFIG;").values())


def get_settings(session):
    return dict(session.run("SHOW DATABASE SETTINGS;").values())


def get_log_file_path(session):
    log_path = get_configs(session)["log_file"]
    if len(log_path) == 0:
        return ""
    assert log_path.endswith(".log")
    log_dir_path = os.path.dirname(log_path)
    pattern = re.compile(log_path[len(log_dir_path) + 1 : -4] + r".*\.log$")
    matching_files = [f for f in Path(log_dir_path).iterdir() if pattern.match(f.name)]
    if not matching_files:
        return ""
    last_created_file = max(matching_files, key=lambda f: f.stat().st_ctime)
    return str(last_created_file.absolute())


def get_log_level(session):
    return get_settings(session)["log.level"]


def set_log_level(session, log_level):
    session.run(f"SET DATABASE SETTING 'log.level' TO '{log_level}';")
