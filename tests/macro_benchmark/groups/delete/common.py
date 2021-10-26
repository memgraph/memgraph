# Copyright 2021 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

""" This file does nothing, it's just utilities for other setups """

from random import randint, seed


BATCH_SIZE = 100
seed(0)


def create_vertices(vertex_count):
    for vertex in range(vertex_count):
        print("CREATE (:Label {id: %d})" % vertex)
        if (vertex != 0 and vertex % BATCH_SIZE == 0) or \
                (vertex + 1 == vertex_count):
            print(";")


def create_edges(edge_count, vertex_count):
    """ vertex_count is the number of already existing vertices in graph """
    matches = []
    merges = []
    for edge in range(edge_count):
        matches.append("MATCH (a%d :Label {id: %d}), (b%d :Label {id: %d})" %
            (edge, randint(0, vertex_count - 1),
             edge, randint(0, vertex_count - 1)))
        merges.append("CREATE (a%d)-[:Type]->(b%d)" % (edge, edge))
        if (edge != 0 and edge % BATCH_SIZE == 0) or \
                ((edge + 1) == edge_count):
            print(" ".join(matches + merges))
            print(";")
            matches = []
            merges = []
