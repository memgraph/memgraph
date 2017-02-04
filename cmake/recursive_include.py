#!/usr/bin/env python

"""
A script for finding and [copying|printing] C++
headers that get recursively imported from one (or more)
starting points.

Supports absolute imports relative to some root folder
(project root) and relative imports relative to the
header that is doing the importing.

Does not support conditional imports (resulting from
#ifdef macros and such). All the #import statements
found in a header (one #import per line) are traversed.

Supports Python2 and Python3.
"""

__author__      = "Florijan Stamenkovic"
__copyright__   = "Copyright 2017, Memgraph"

import logging
import sys
import os
import re
import shutil
from argparse import ArgumentParser

# the prefix of an include directive
PREFIX = "#include"

log = logging.getLogger(__name__)


def parse_args():
    argp = ArgumentParser(description=__doc__)
    argp.add_argument("--logging", default="INFO", choices=["INFO", "DEBUG"],
            help="Logging level")
    argp.add_argument("--root", required=True,
            help="Root path of the header tree (project root)")
    argp.add_argument("--start", required=True, nargs="+",
            help="One or more headers from which to start scanning")
    argp.add_argument("--stdout", action="store_true",
            help="If found paths should be printed out to stdout")
    argp.add_argument("--copy", default=None,
            help="Prefix of the path where the headers should be copied")
    return argp.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(level=args.logging)

    log.info("Recursively detecting used C/C++ headers in root '%s' with starting point(s) '%s'",
            args.root, args.start)

    results = set()
    for start in args.start:
        find_recursive(start, args.root, results)

    results = list(sorted(results))

    log.debug("Found %d paths:", len(results))
    for r in results:
        log.debug("\t%s", r)

    # print out the results if required
    if args.stdout:
        for result in results:
            print(result)

    # copy the results if required
    if args.copy is not None:
        for result in results:
            from_path = os.path.join(args.root, result)
            to_path = os.path.join(args.copy, result)
            log.debug("Copying '%s' to '%s'", from_path, to_path)

            #   create a directory if necessary, Py2 and Py3 compatible
            to_dir = os.path.dirname(to_path)
            if not os.path.exists(to_dir):
                os.makedirs(to_dir)

            shutil.copy(from_path, to_path)


def find_recursive(path, project_root, results):
    """
    Recursivelly looks for headers and adds them to results.
    The headers are added as paths relative to the given
    `project_root`

    Args:
        path: str, path to a header. This header is added
            to results and scanned for #include statements of
            other headers. For each #include (relative to current
            `path` or to `project_root`) for which a file is found
            this same function is called.
        project_root: str, path to a project root. Used for
            finding headers included using an absolute path
            (that is actually relative to project_root).
        results: a collection into which the results are
            added.
    """
    log.debug("Processing path: %s", path)

    path_abs = os.path.abspath(path)
    root_abs = os.path.abspath(project_root)

    if not path_abs.startswith(root_abs):
        log.warning("Project root '%s' not prefix of path '%s'",
                    root_abs, path_abs)

    path_rel = path_abs[len(root_abs) + 1:]
    log.debug("Rel path is '%s'", path_rel)

    if path_rel in results:
        log.debug("Skipping already present path '%s'", path_rel)
        return

    log.debug("Adding path '%s'", path_rel)
    results.add(path_rel)

    # go through files and look for include directives
    with open(path_abs) as f:
        for line in filter(lambda l: l.startswith(PREFIX),
                           map(lambda l: l.strip(), f)):

            include = line[len(PREFIX):].strip()
            include = re.sub("[\"\']", "", include)

            log.debug("Processing include '%s'", include)

            # check if the file exists relative to this file
            # or absolutely to project root
            include_abs = os.path.join(project_root, include)
            if os.path.exists(include_abs):
                find_recursive(include_abs, project_root, results)

            include_rel = os.path.join(os.path.dirname(path_abs), include)
            if os.path.exists(include_rel):
                find_recursive(include_rel, project_root, results)


if __name__ == '__main__':
    main()
