#!/usr/bin/python3

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
import time
import json
from argparse import ArgumentParser
from contextlib import redirect_stderr
import io

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


def main():
    argp = ArgumentParser("Bolt client execution process")
    # positional args
    argp.add_argument("db_uri")
    argp.add_argument("queries", nargs="*")
    # named, optional
    argp.add_argument("--encrypt", action="store_true")

    # parse ags, ensure that stdout is not polluted by argument parsing
    try:
        f = io.StringIO()
        with redirect_stderr(f):
            args = argp.parse_args()
    except:
        _print_dict({RETURN_CODE: 1, ERROR_MSG: "Invalid cmd-line arguments"})
        sys.exit(1)

    driver = GraphDatabase.driver(
        args.db_uri,
        auth=basic_auth("", ""),
        encrypted=args.encrypt)

    session = driver.session()

    # execute the queries
    metadatas = []
    start = time.time()
    for query in args.queries:
        result = session.run(query)
        metadatas.append(result.summary().metadata)
    end = time.time()
    delta_time = end - start

    _print_dict({
        RETURN_CODE: 0,
        WALL_TIME: (None if not args.queries else
                    delta_time / float(len(args.queries))),
        "metadatas": metadatas
    })

    session.close()
    driver.close()


if __name__ == '__main__':
    main()
