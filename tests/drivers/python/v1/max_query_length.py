#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost:7687",
                              auth=basic_auth("", ""),
                              encrypted=False)

query_template = 'CREATE (n {name:"%s"})'
template_size = len(query_template) - 2  # because of %s
min_len = 1
max_len = 1000000

# binary search because we have to find the maximum size (in number of chars)
# of a query that can be executed via driver
while True:
    assert min_len > 0 and max_len > 0, \
        "The lengths have to be positive values! If this happens something" \
        " is terrible wrong with min & max lengths OR the database" \
        " isn't available."
    property_size = (max_len + min_len) // 2
    try:
        driver.session().run(query_template % ("a" * property_size)).consume()
        if min_len == max_len or property_size + 1 > max_len:
            break
        min_len = property_size + 1
    except Exception as e:
        print("Query size %s is too big!" % (template_size + property_size))
        max_len = property_size - 1

assert property_size == max_len, "max_len probably has to be increased!"

print("\nThe max length of a query from Python driver is: %s\n" %
      (template_size + property_size))

# sessions are not closed bacause all sessions that are
# executed with wrong query size might be broken
driver.close()
