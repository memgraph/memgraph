#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

# TODO: auto import
from scenario import no_000001 
from neo4j.v1 import GraphDatabase, basic_auth, types, Node

scenarios = [no_000001.scenario_1]

# logging init
log = logging.getLogger(__name__)

# initialize driver and create session
session = GraphDatabase.driver("bolt://localhost",
                              auth=basic_auth("neo4j", "1234"),
                              encrypted=0).session()


def check(condition, scenario_no, message):
	'''
	Checks condition if condition is false the whole test will be stopped and
	scenario_no + message will be printed out.

	:param condition: bool
	:param scenario_no: int
	:param message: assert message
	:returns: None
	'''
	if not condition:
		log.error("Error in scenario: %s" % scenario_no)
		assert condition, message


if __name__ == "__main__":

	logging.basicConfig(level=logging.DEBUG)

	for scenario in scenarios:

		# iterate through all queries and execute them agains the database
		for query, result in scenario.queries:

			# extract count and properties from test entries
			count, expected = result
			records = [record for record in session.run(query)]

			log.info("%s" % records)

			# check count
			check(len(records) == count, scenario.no,
				  "Number of results for %s isn't good;" \
				  " expected: %s, got %s" % (query, count, len(records)))

			# in case that the result is single
			if count == 1:
				# extract properties from record
				record = records[0]
				record_name, = record
				data = record[record_name]

				if type(data) is Node:
					received = {k: v for (k, v) in data.items()}
				else:
					received = data
				
				# get expected elements
				expected = expected[0]

				# check properties
				check(expected == received, scenario.no,
					  "Received data for %s isn't OK; expected: %s, " \
					  "got %s" % (query, expected, received))

			elif count > 1:
				# TODO: implement the full test
				pass
