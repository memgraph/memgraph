#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Marko Budiselic"

class Scenario1:
    '''
    Create Match Retrun Sequence
    '''

    def __init__(self):
        '''
        Constructor
        '''
        self.no = 1
        self.desctiption = "Create Match Retrun Sequence"
        self.init_graph = []
        self.queries = [
			("MATCH (n) DETACH DELETE n", (0, None)),
			("MATCH (n) RETURN n", (0, [])),
			("CREATE (n:Label) RETURN n", (1, [{}])),
			("MATCH (n) RETURN n", (1, [{}])),
			("CREATE (n:Label {prop: 0}) RETURN n", (1, [{"prop": 0}])),
			("MATCH (n) RETURN n", (2, [{}, {"prop": 0}])),
            ("CREATE (n:Label {prop: 1}) RETURN n", (1, [{"prop": 1}])),
            ("MATCH (n) RETURN n", (3, [{}, {"prop": 0}, {"prop": 1}])),
            ("CREATE (n {prop: 1}) RETURN n", (1, [{"prop": 1}])),
            ("MATCH (n) RETURN n", (4, [{}, {"prop": 0}, {"prop": 1}, {"prop": 0}])),
            ("MATCH (n:Label) RETURN n", (3, [{}, {"prop": 0}, {"prop": 1}])),
            ("MATCH (n:Label {prop: 0}) RETURN n", (1, [{"prop": 0}])),
		]
        self.side_effects = None

scenario_1 = Scenario1()
