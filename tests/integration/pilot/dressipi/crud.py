#!/usr/bin/env python
# -*- coding: utf-8 -*-

from neo4j.v1 import GraphDatabase, basic_auth, types

# initialize driver and create session
# default username and password are used
driver = GraphDatabase.driver("bolt://localhost",
                              auth=basic_auth("neo4j", "neo4j"),
                              encrypted=0)
session = driver.session()

# all test queries and expected results
# one element in queries is called test entry
# it contains test query +
# touple(number of expected results, expected properties)
# TODO: create custom data structures
queries = [
    ("CREATE (n:Garment {garment_id: 1234, garment_category_id: 1}) RETURN n",
     (1, [{"garment_id": 1234, "garment_category_id": 1}])),
    ("CREATE(p:Profile {profile_id: 111, partner_id: 55}) RETURN p",
     (1, [{"profile_id": 111, "partner_id": 55}])),
    # ("MATCH (p:Profile) RETURN p",
    #  (1, [{"profile_id": 111, "partner_id": 55}])),
    ("MATCH (n) DELETE n",
     (0, []))
];

# iterate through all queries and execute them agains the database
for query, result in queries:

    # extract count and properties from test entries
    count, test_properties = result
    records = [record for record in session.run(query)]

    # check count
    assert len(records) == count, \
            "Number of results for %s isn't good;" \
            " expected: %s, got %s" % (query, count, len(records))

    # in case that result should contain just one result
    # test properties
    # TODO: test others
    if count == 1:
        # extract properties from record
        record = records[0]
        record_name, = record
        received_properties = {key: value
                               for (key, value) in record[record_name].items()}
        
        # get expected properties
        expected_properties = test_properties[0]

        # check properties
        assert expected_properties == received_properties, \
                "Received properties for %s are not good; expected: %s, " \
                "got %s" % (query, expected_properties, received_properties)

print("Dressipi integration test passed OK")
