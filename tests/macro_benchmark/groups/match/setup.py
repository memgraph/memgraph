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

"""
Generates a random graph with some configurable statistics.
"""

from random import randint, seed


seed(0)

def rint(upper_bound_exclusive):
    return randint(0, upper_bound_exclusive - 1)

VERTEX_COUNT = 1500
EDGE_COUNT = VERTEX_COUNT * 15

# numbers of *different* labels, edge types and properties
LABEL_COUNT = 10

MAX_LABELS = 5  # maximum number of labels in a vertex
MAX_PROPS = 4   # maximum number of properties in a vertex/edge
MAX_PROP_VALUE = 1000

# some consts used in multiple files
LABEL_INDEX = "LabelIndex"
LABEL_PREFIX = "Label"
PROP_PREFIX = "Prop"
ID = "id"



def labels():
    labels = ":" + LABEL_INDEX
    for _ in range(rint(MAX_LABELS)):
        labels += ":" + LABEL_PREFIX + str(rint(LABEL_COUNT))
    return labels


def properties(id):
    """ Generates a properties string with [0, MAX_PROPS) properties.
    Note that if PropX is generated, then all the PropY where Y < X
    are generated. Thus most labels have Prop0, and least have PropMAX_PROPS.
    """
    props = {"%s%d" % (PROP_PREFIX, i): rint(MAX_PROP_VALUE)
             for i in range(rint(MAX_PROPS))}
    props[ID] = id
    return "{" + ", ".join("%s: %s" % kv for kv in props.items()) + "}"


def vertex(vertex_index):
    return "(%s %s)" % (labels(), properties(vertex_index))


def main():
    # create an index to speed setup up
    print("CREATE INDEX ON :" + LABEL_INDEX + ";")
    for i in range(LABEL_COUNT):
        print("CREATE INDEX ON :" + LABEL_PREFIX + str(i) + ";")
    print("CREATE INDEX ON :%s(%s);" % (LABEL_INDEX, ID))

    # we batch CREATEs because to speed creation up
    BATCH_SIZE = 30

    # create vertices
    for vertex_index in range(VERTEX_COUNT):
        print("CREATE %s" % vertex(vertex_index))
        if (vertex_index != 0 and vertex_index % BATCH_SIZE == 0) or \
                vertex_index + 1 == VERTEX_COUNT:
            print(";")
    print("MATCH (n) RETURN assert(count(n) = %d);" % VERTEX_COUNT)

    # create edges stohastically
    attempts = VERTEX_COUNT ** 2
    p = EDGE_COUNT / VERTEX_COUNT ** 2
    print("MATCH (a) WITH a MATCH (b) WITH a, b WHERE rand() < %f "
          " CREATE (a)-[:EdgeType]->(b);" % p)
    sigma = (attempts * p * (1 - p)) ** 0.5
    delta = 5 * sigma
    print("MATCH (n)-[r]->() WITH count(r) AS c "
          "RETURN assert(c >= %d AND c <= %d);" % (
            EDGE_COUNT - delta, EDGE_COUNT + delta))


if __name__ == "__main__":
    main()
