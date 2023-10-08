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

import logging
from typing import Dict

from constants import (
    COUNT,
    CPU,
    DATABASE,
    DOCKER,
    DURATION,
    IMPORT,
    ITERATIONS,
    LATENCY_STATS,
    MEMORY,
    METADATA,
    RETRIES,
    RUN_CONFIGURATION,
    THROUGHPUT,
)

COLOR_GRAY = 0
COLOR_RED = 1
COLOR_GREEN = 2
COLOR_YELLOW = 3
COLOR_BLUE = 4
COLOR_VIOLET = 5
COLOR_CYAN = 6
COLOR_WHITE = 7


logger = logging.Logger("mgbench_logger")
file_handler = logging.FileHandler("mgbench_logs.log")
file_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(file_format)
logger.addHandler(file_handler)


def _log(color, *args):
    print("\033[1;3{}m~~".format(color), *args, "~~\033[0m")


def log(msg):
    print(str(msg))
    logger.info(msg=msg)


def init(*args):
    _log(COLOR_BLUE, *args)
    logger.info(*args)


def info(*args):
    _log(COLOR_WHITE, *args)
    logger.info(*args)


def success(*args):
    _log(COLOR_GREEN, *args)
    logger.info(*args)


def warning(*args):
    _log(COLOR_YELLOW, *args)
    logger.warning(*args)


def error(*args):
    _log(COLOR_RED, *args)
    logger.critical(*args)


def summary(*args):
    _log(COLOR_CYAN, *args)
    logger.info(*args)
