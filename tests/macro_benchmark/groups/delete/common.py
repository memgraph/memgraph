""" This file does nothing, it's just utilities for other setups """

from random import randint


BATCH_SIZE = 100


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
