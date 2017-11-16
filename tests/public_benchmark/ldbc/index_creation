#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import sys

from neo4j.v1 import GraphDatabase, basic_auth

def parse_args():
    argp = argparse.ArgumentParser(
            description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    argp.add_argument('indexfile', help='File with indexes that should be created.')
    argp.add_argument('--host', default='127.0.0.1', help='Database host.')
    argp.add_argument('--port', default='7687', help='Database port.')
    argp.add_argument('--username', default='', help='Database username.')
    argp.add_argument('--password', default='', help='Database password.')
    argp.add_argument('--database', default='memgraph',
                      choices=('memgraph', 'neo4j'), help='Database used.')
    return argp.parse_args()

# Parse args
args = parse_args()

# Initialize driver and create session.
driver = GraphDatabase.driver('bolt://{}:{}'.format(args.host, args.port),
                              auth=basic_auth(args.username, args.password),
                              encrypted=False)
session = driver.session()

# The fist program argument is path to a file with indexes.
with open(args.indexfile, "r") as f:
    print("Starting index creation...")
    for line in f.readlines():
        session.run(line.strip()).consume()
        print("%s -> DONE" % line.strip())
    if args.database == "neo4j":
        print("Waiting for indexes to be fully created...")
        session.run("CALL db.awaitIndexes(14400);").consume()
    print("All indexes were created.")

# Do the cleanup.
session.close()
driver.close()
