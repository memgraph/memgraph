#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Marko Budiselic"
__date__   = "2016_11_03"

class Scenario2:
    '''
    Create node, edit labels and return all labels for the node.
    '''

    def __init__(self):
        '''
        Constructor
        '''
        self.no = 2
        self.desctiption = "Create node, edit labels and return all labels for the node."
        self.init_graph = []
        self.queries = [
            ("MATCH (n) DETACH DELETE n", (0, None)),
            ("CREATE (n:Garment {garment_id: 1234, garment_category_id: 1}) RETURN n", (1, [{"garment_id": 1234, "garment_category_id": 1}])),
            ("MATCH (g:Garment {garment_id: 1234}) SET g:FF RETURN labels(g)", (1, [["Garment", "FF"]])),
            ("MATCH(g:Garment {garment_id: 1234}) SET g.reveals = 50 RETURN g", (1, [{"garment_id": 1234, "garment_category_id": 1, "reveals": 50}])),
            ("MATCH (n) RETURN n", (1, [{"garment_id": 1234, "garment_category_id": 1, "reveals": 50}])),
        ]
        self.side_effects = None

scenario_2 = Scenario2()
