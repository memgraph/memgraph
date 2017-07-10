""" This file does nothing, it's just utilities for other setups """

from random import randint


BATCH_SIZE = 50


def create_vertices(vertex_count):
    for vertex in range(vertex_count):
        print("CREATE (:Label {id: %d})" % vertex)
        if vertex != 0 and vertex % BATCH_SIZE == 0:
            print(";")


def create_edges(edge_count, vertex_count):
    """ vertex_count is the number of already existing vertices in graph """
    for edge in range(edge_count):
        print("MERGE ({id: %d})-[:Type]->({id: %d})" % (
            randint(0, vertex_count - 1), randint(0, vertex_count - 1)))
        if edge != 0 and edge % BATCH_SIZE == 0:
            print(";")


if __name__ == '__main__':
    raise Exception("This file is just for utilities, not for actual setup")
