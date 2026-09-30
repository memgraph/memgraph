from typing import List

import mgp
from mage.geography import create_distance_matrix, solve_1_5_approx, solve_2_approx, solve_greedy

DEFAULT_SOLVING_METHOD = "1.5_approx"
tsp_solving_methods = {
    "2_approx": solve_2_approx,
    "greedy": solve_greedy,
    DEFAULT_SOLVING_METHOD: solve_1_5_approx,
}


@mgp.read_proc
def solve(
    context: mgp.ProcCtx, points: List[mgp.Vertex], method: str = DEFAULT_SOLVING_METHOD
) -> mgp.Record(sources=List[mgp.Vertex], destinations=List[mgp.Vertex]):
    """
    The tsp solver returns 2 fields whose elements at indexes are correlated

      * `sources` - elements from 1st to n-1th element
      * `destinations` - elements from 2nd to nth element

    The pairs of them represent individual edges between 2 nodes in the graph.

    The required argument is the list of cities one wants to find the path from.
    The optional argument `method` is by default 'greedy'. Other arguments that can be
    specified are '2-approx' and '1.5-approx'

    The procedure can be invoked in openCypher using the following calls:
    MATCH (n:Point)
    WITH collect(n) as points
    CALL tsp.solve(points) YIELD sources, destinations RETURN sources, destinations;
    """

    if not all(isinstance(x, mgp.Vertex) for x in points):
        return mgp.Record(sources=None, destinations=None)

    dm = create_distance_matrix([dict(x.properties.items()) for x in points])

    if dm is None:
        return mgp.Record(sources=None, destinations=None)

    if method.lower() not in tsp_solving_methods.keys():
        method = DEFAULT_SOLVING_METHOD

    order = tsp_solving_methods[method](dm)

    sources = [points[order[x]] for x in range(len(order) - 1)]
    destinations = [points[order[x]] for x in range(1, len(order))]

    return mgp.Record(sources=sources, destinations=destinations)
