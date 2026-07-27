from enum import Enum
from itertools import product
from typing import Any, Tuple

import mgp
from mage.union_find import DisjointSet

disjoint_set = DisjointSet(node_ids=None)


class Mode(Enum):
    """
    Valid values of the mode parameter
    """

    CARTESIAN = "cartesian"
    PAIRWISE = "pairwise"


@mgp.read_proc
def connected(
    ctx: mgp.ProcCtx,
    nodes1: Any,
    nodes2: Any,
    mode: str = "pairwise",
    update: bool = True,
) -> mgp.Record(node1=mgp.Vertex, node2=mgp.Vertex, connected=bool):
    """
    Returns whether two nodes (or each pair in the product of two node lists) belong to the same connected component
    of the graph.
    :param nodes1: Node or tuple of nodes
    :type nodes1: Union[mgp.Vertex, Tuple[mgp.Vertex]]
    :param nodes2: Node or tuple of nodes
    :type nodes2: Union[mgp.Vertex, Tuple[mgp.Vertex]]
    :param mode: Mode of operation: `pairwise` for pairwise operation or `cartesian`
    for operating on the Cartesian product of given tuples. Default value is `pairwise`.
    :type mode: str
    :param update: Updates the disjoint set data structure used by the algorithm. Use if graph has been changed
    since this method's last call. Default value is `True`.
    :type update: str

    :return:
    :rtype: mgp.Record
    """

    if update:
        disjoint_set.reinitialize(node_ids=[vertex.id for vertex in ctx.graph.vertices])

        for vertex in ctx.graph.vertices:
            for edge in vertex.out_edges:
                disjoint_set.union(node1_id=edge.from_vertex.id, node2_id=edge.to_vertex.id)

    if isinstance(nodes1, mgp.Vertex):
        nodes1 = tuple([nodes1])
    elif isinstance(nodes1, Tuple):
        pass
    else:
        raise TypeError("Invalid type of first argument.")

    if isinstance(nodes2, mgp.Vertex):
        nodes2 = tuple([nodes2])
    elif isinstance(nodes2, Tuple):
        pass
    else:
        raise TypeError("Invalid type of second argument.")

    if mode.lower() == Mode.PAIRWISE.value:
        if len(nodes1) != len(nodes2):
            raise ValueError("Incompatible lengths of given arguments.")

        return [
            mgp.Record(
                node1=node1,
                node2=node2,
                connected=disjoint_set.connected(node1_id=node1.id, node2_id=node2.id),
            )
            for node1, node2 in zip(nodes1, nodes2)
        ]

    elif mode.lower() == Mode.CARTESIAN.value:
        return [
            mgp.Record(
                node1=node1,
                node2=node2,
                connected=disjoint_set.connected(node1_id=node1.id, node2_id=node2.id),
            )
            for node1, node2 in product(nodes1, nodes2)
        ]

    error_message = f'Mode {mode} is invalid, please specify one of the following: "{Mode.PAIRWISE.value}", "{Mode.CARTESIAN.value}".'
    raise ValueError(error_message)
