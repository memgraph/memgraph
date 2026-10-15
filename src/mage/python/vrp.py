from typing import Dict, List

import mgp
from mage.constraint_programming import VRPConstraintProgrammingSolver
from mage.geography import LATITUDE, LONGITUDE, create_distance_matrix

__distance_matrix = None
__depot_index = None

MAX_DISTANCE_MATRIX_SIZE = 100


def get_distance_matrix(vertices):
    """
    Assigns distance matrix global object or returns if its already there.
    """
    global __distance_matrix

    if __distance_matrix is not None:
        return __distance_matrix

    vertex_positions: List[Dict[str, float]] = []
    for vertex in vertices:
        vertex_positions.append(
            {
                LATITUDE: vertex.properties.get(LATITUDE),
                LONGITUDE: vertex.properties.get(LONGITUDE),
            }
        )

    __distance_matrix = create_distance_matrix(vertex_positions)

    return __distance_matrix


def get_depot_index(vertices: mgp.Vertices, depot_node: mgp.Vertex):
    """
    Assigns depot index global variable or returns if its already there.
    """
    global __depot_index

    if __depot_index is not None:
        return __depot_index

    for i, vertex in enumerate(vertices):
        if vertex == depot_node:
            __depot_index = i
            break

    if __depot_index is None:
        raise DepotUnspecifiedException("No depot location specified!")

    return __depot_index


def cleanup():
    global __distance_matrix, __depot_index

    if __distance_matrix is not None and len(__distance_matrix) >= MAX_DISTANCE_MATRIX_SIZE:
        __distance_matrix = None
        __depot_index = None


@mgp.read_proc
def route(
    context: mgp.ProcCtx,
    depot_node: mgp.Vertex,
    number_of_vehicles: mgp.Nullable[int] = None,
) -> mgp.Record(from_vertex=mgp.Vertex, to_vertex=mgp.Vertex):
    """
    The VRP routing returns 2 fields.
        * `from_vertex` represents the starting nodes out of all selected routes (edges) in the complete graph
        * `to_vertex` represents the ending nodes out of all selected routes (edges) in the complete graph

    The input arguments are:
        * `number_of_vehicle` represents the cardinality of fleet with which the problem is going to be solved
        * `depot_label` represents the name of the label which contains the depot node
    """

    if number_of_vehicles is None:
        number_of_vehicles = 1
    if number_of_vehicles <= 0:
        raise Exception("Number of vehicles must be greater than 0.")

    vertices = [v for v in context.graph.vertices]
    distance_matrix = get_distance_matrix(vertices)
    depot_index = get_depot_index(vertices, depot_node)

    solver = VRPConstraintProgrammingSolver(number_of_vehicles, distance_matrix, depot_index)
    solver.solve()

    result = solver.get_result()

    cleanup()

    return [mgp.Record(from_vertex=vertices[x.from_vertex], to_vertex=vertices[x.to_vertex]) for x in result.vrp_paths]


class DepotUnspecifiedException(Exception):
    pass
