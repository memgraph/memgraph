"""
Generates a random graph with some configurable statistics.
"""

from random import randint


def rint(upper_bound_exclusive):
    return randint(0, upper_bound_exclusive - 1)

VERTEX_COUNT = 10000
EDGE_COUNT = VERTEX_COUNT * 3

# numbers of *different* labels, edge types and properties
LABEL_COUNT = 10
EDGE_TYPE_COUNT = 10

MAX_LABELS = 3  # maximum number of labels in a vertex
MAX_PROPS = 4   # maximum number of properties in a vertex/edge

# some consts used in mutiple files
LABEL_PREFIX = "Label"
PROP_PREFIX = "Property"
ID = "id"


def labels():
    return "".join(":%s%d" % (LABEL_PREFIX, rint(LABEL_COUNT))
                   for _ in range(randint(1, MAX_LABELS - 1)))


def properties(id):
    """ Generates a properties string with [0, MAX_PROPS) properties.
    Note that if PropX is generated, then all the PropY where Y < X
    are generated. Thus most labels have Prop0, and least have PropMAX_PROPS.
    """
    return "{%s: %d, %s}" % (ID, id, ",".join(
        ["%s%d: %d" % (PROP_PREFIX, prop_ind, rint(100))
         for prop_ind in range(randint(1, MAX_PROPS - 1))]))


def vertex(vertex_index):
    return "(%s %s)" % (labels(), properties(vertex_index))


def edge(edge_index):
    return "[:EdgeType%d %s]" % (rint(EDGE_TYPE_COUNT), properties(edge_index))


def main():
    # we batch CREATEs because to speed creation up
    BATCH_SIZE = 50

    # create vertices
    for vertex_index in range(VERTEX_COUNT):
        print("CREATE %s" % vertex(vertex_index))
        if (vertex_index != 0 and vertex_index % BATCH_SIZE == 0) or \
                vertex_index + 1 == VERTEX_COUNT:
            print(";")

    # create edges
    for edge_index in range(EDGE_COUNT):
        print("MATCH (a {%s: %d}), (b {%s: %d}) MERGE (a)-%s->(b)" % (
            ID, randint(0, VERTEX_COUNT - 1),
            ID, randint(0, VERTEX_COUNT - 1),
            edge(edge_index)))
        print(";")


if __name__ == "__main__":
    main()
