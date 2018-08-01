#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from behave.__main__ import main as behave_main
from behave import configuration
from argparse import ArgumentParser
import os
import sys


def parse_args(argv):
    argp = ArgumentParser(description=__doc__)
    argp.add_argument("--root", default="tck_engine/tests/memgraph_V1",
                      help="Path to folder where tests are located, default "
                           "is tck_engine/tests/memgraph_V1")
    argp.add_argument(
        "--stop", action="store_true", help="Stop testing after first fail.")
    argp.add_argument("--side-effects", action="store_false",
                      help="Check for side effects in tests.")
    argp.add_argument("--db", default="memgraph",
                      choices=["neo4j", "memgraph"],
                      help="Default is memgraph.")
    argp.add_argument("--db-user", default="neo4j", help="Default is neo4j.")
    argp.add_argument(
        "--db-pass", default="1234", help="Default is 1234.")
    argp.add_argument("--db-uri", default="bolt://127.0.0.1:7687",
                      help="Default is bolt://127.0.0.1:7687.")
    argp.add_argument("--output-folder", default="tck_engine/results/",
                      help="Test result output folder, default is results/.")
    argp.add_argument("--logging", default="DEBUG", choices=["INFO", "DEBUG"],
                      help="Logging level, default is DEBUG.")
    argp.add_argument("--unstable", action="store_true",
                      help="Include unstable feature from features.")
    argp.add_argument("--single-fail", action="store_true",
                      help="Pause after failed scenario.")
    argp.add_argument("--single-scenario", action="store_true",
                      help="Pause after every scenario.")
    argp.add_argument("--single-feature", action="store_true",
                      help="Pause after every feature.")
    argp.add_argument("--test-name", default="",
                      help="Name of the test")
    argp.add_argument("--distributed", action="store_true",
                      help="Run memgraph in distributed")
    argp.add_argument("--num-machines", type=int, default=3,
                      help="Number of machines for distributed run")
    argp.add_argument("--memgraph-params", default="",
                      help="Additional params for memgraph run")
    return argp.parse_args(argv)


def add_config(option, dictionary):
    configuration.options.append(
        ((option,), dictionary)
    )


def main(argv):
    """
    Script used to run behave tests with given options. List of
    options is available when running python test_executor.py -help.
    """
    args = parse_args(argv)

    tests_root = os.path.abspath(args.root)

    # adds options to cucumber configuration
    add_config("--side-effects",
               dict(action="store_false", help="Exclude side effects."))
    add_config("--database", dict(help="Choose database(memgraph/neo4j)."))
    add_config("--database-password", dict(help="Database password."))
    add_config("--database-username", dict(help="Database username."))
    add_config("--database-uri", dict(help="Database uri."))
    add_config("--output-folder", dict(
        help="Folder where results of tests are written."))
    add_config("--root", dict(help="Folder with test features."))
    add_config("--single-fail",
               dict(action="store_true", help="Pause after failed scenario."))
    add_config("--single-scenario",
               dict(action="store_true", help="Pause after every scenario."))
    add_config("--single-feature",
               dict(action="store_true", help="Pause after every feature."))
    add_config("--test-name", dict(help="Name of the test."))
    add_config("--distributed",
               dict(action="store_true", help="Run memgraph in distributed."))
    add_config("--num-machines",
               dict(help="Number of machines for distributed run."))
    add_config("--memgraph-params", dict(help="Additional memgraph params."))

    # list with all options
    # options will be passed to the cucumber engine
    behave_options = [tests_root]
    if args.stop:
        behave_options.append("--stop")
    if args.side_effects:
        behave_options.append("--side-effects")
    if args.db != "memgraph":
        behave_options.append("-e")
        behave_options.append("memgraph*")
    if not args.unstable:
        behave_options.append("-e")
        behave_options.append("unstable*")
    behave_options.append("--database")
    behave_options.append(args.db)
    behave_options.append("--database-password")
    behave_options.append(args.db_pass)
    behave_options.append("--database-username")
    behave_options.append(args.db_user)
    behave_options.append("--database-uri")
    behave_options.append(args.db_uri)
    behave_options.append("--root")
    behave_options.append(args.root)
    if (args.single_fail):
        behave_options.append("--single-fail")
    if (args.single_scenario):
        behave_options.append("--single-scenario")
    if (args.single_feature):
        behave_options.append("--single-feature")
    if (args.distributed):
        behave_options.append("--distributed")
        behave_options.append("--num-machines")
        behave_options.append(str(args.num_machines))
    behave_options.append("--output-folder")
    behave_options.append(args.output_folder)
    behave_options.append("--test-name")
    behave_options.append(args.test_name)
    if (args.memgraph_params):
        behave_options.append("--memgraph-params")
        behave_options.append(args.memgraph_params)

    # runs tests with options
    return behave_main(behave_options)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
