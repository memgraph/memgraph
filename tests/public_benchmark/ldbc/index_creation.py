#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys

from neo4j.v1 import GraphDatabase, basic_auth

# Initialize driver and create session.
driver = GraphDatabase.driver('bolt://localhost:7687',
                              auth=basic_auth('', ''),
                              encrypted=False)
session = driver.session()

# The fist program argument is path to a file with indexes.
try:
    with open(sys.argv[1], "r") as f:
        for line in f.readlines():
            session.run(line.strip()).consume()
            print("%s -> DONE" % line.strip())
        print("All indexes were created.")
except:
    print("Frist argument is path to a file with indexes.")

# Do the cleanup.
session.close()
driver.close()
