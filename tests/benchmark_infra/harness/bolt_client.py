#!/usr/bin/env python3

"""
A python script that launches the memgraph client,
executes a query and prints out a JSON dict of measurements
to stdout.

Takes a number of cmd-line arguments of the following structure:
    Positional, mandatory:
        - db_uri
        - query
    Named, optional:
        - encrypt

Required the database URI to be passed as the single
cmd line argument.

The dict that is printed out contains:
    - return_code of the client execution process
    - error_msg (empty if not applicable)
    - metedata dict

Note that 'metadata' are only valid if the return_code is 0
"""

import sys
import os
# tests/stress dir, that's the place of common.py.
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.realpath(__file__)))), "stress"))
import time
import json
from argparse import ArgumentParser
from contextlib import redirect_stderr
import io
from multiprocessing import Pool
from common import connection_argument_parser, execute_till_success, \
        argument_session
from functools import partial

from neo4j.v1 import GraphDatabase, basic_auth

# string constants
RETURN_CODE = "return_code"
ERROR_MSG = "error_msg"
WALL_TIME = "wall_time"

def _prepare_for_json(obj):
    if isinstance(obj, dict):
        return {k: _prepare_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_prepare_for_json(elem) for elem in obj]
    if isinstance(obj, (str, int, float, type(None))):
        return obj
    return None


def _print_dict(d):
    print(json.dumps(_prepare_for_json(d), indent=2))


def _run_query(args, query):
    with argument_session(args) as session:
        return execute_till_success(session, query)[2]


def main():
    argp = connection_argument_parser()
    argp.add_argument("--num-workers", type=int, default=1)

    # Parse args and ensure that stdout is not polluted by argument parsing.
    try:
        f = io.StringIO()
        with redirect_stderr(f):
            args = argp.parse_args()
    except:
        _print_dict({RETURN_CODE: 1, ERROR_MSG: "Invalid cmd-line arguments"})
        sys.exit(1)

    queries = sys.stdin.read().split("\n")

    # Execute the queries.
    metadatas = []
    with Pool(args.num_workers) as pool:
        start = time.time()
        metadatas = list(pool.map(partial(_run_query, args), queries))
        end = time.time()
        delta_time = end - start

    _print_dict({
        RETURN_CODE: 0,
        WALL_TIME: (None if not queries else delta_time),
        "metadatas": metadatas
    })


if __name__ == '__main__':
    main()
