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

COLOR_GRAY = 0
COLOR_RED = 1
COLOR_GREEN = 2
COLOR_YELLOW = 3
COLOR_BLUE = 4
COLOR_VIOLET = 5
COLOR_CYAN = 6


def log(color, *args):
    print("\033[1;3{}m~~".format(color), *args, "~~\033[0m")


def init(*args):
    log(COLOR_BLUE, *args)


def info(*args):
    log(COLOR_CYAN, *args)


def success(*args):
    log(COLOR_GREEN, *args)


def warning(*args):
    log(COLOR_YELLOW, *args)


def error(*args):
    log(COLOR_RED, *args)
