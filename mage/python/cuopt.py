"""
cuOpt query module for Memgraph.

Provides cuopt.vrp(depot_nodes, config) to solve a multi-vehicle routing problem on
an external, GPU-backed cuOpt server (https://github.com/NVIDIA/cuopt). This module
is a thin client only: the solve itself happens on the cuopt-server, so no GPU is
needed wherever Memgraph/MAGE runs.

Requires: pip install --extra-index-url https://pypi.nvidia.com cuopt-sh-client
Configure: set CUOPT_SERVER_HOST / CUOPT_SERVER_PORT env vars, or pass host/port in
`config`, pointing at a running cuopt-server instance.
"""

import os
from typing import List

import mgp
from mage.cuopt import CuOptClient, CuOptConnectionError, CuOptSolverException, build_vrp_request, parse_vrp_response
from mage.geography import LATITUDE, LONGITUDE, create_distance_matrix

_DEFAULT_HOST = "localhost"
_DEFAULT_PORT = 5000


@mgp.read_proc
def vrp(
    context: mgp.ProcCtx,
    depot_nodes: List[mgp.Vertex],
    config: mgp.Map = {},
) -> mgp.Record(vehicle_id=int, step=int, from_vertex=mgp.Vertex, to_vertex=mgp.Vertex):
    """
    Solve a multi-vehicle routing problem (VRP/PDP) on an external cuOpt server.

    One vehicle is used per node in `depot_nodes`, each starting and ending at its own
    depot; every other node in the queried graph is treated as a task that must be
    visited. Node positions come from `lat`/`lng` properties, same as vrp.route/tsp.solve.

    `config` keys (all optional):
      * host / port              - cuopt-server address (default: CUOPT_SERVER_HOST /
                                    CUOPT_SERVER_PORT env vars, else localhost:5000)
      * capacities                - single capacity dimension per vehicle, e.g. [10, 10]
      * demand_property           - vertex property name holding each task's demand;
                                    required if `capacities` is set
      * repoll_tries               - max number of repoll attempts for a busy solver
                                    (default 50)
      * repoll_interval_seconds     - seconds to wait between repolls (default 1.0)

    Returns one record per route edge, tagged with which vehicle it belongs to and its
    step order, so a full route can be reconstructed with `ORDER BY vehicle_id, step`.

    Example Cypher:
        MATCH (d:Depot) WITH collect(d) as depots
        CALL cuopt.vrp(depots, {host: "10.0.0.5", port: 5000, capacities: [10, 10],
                                 demand_property: "demand"})
        YIELD vehicle_id, step, from_vertex, to_vertex
        RETURN vehicle_id, step, from_vertex, to_vertex ORDER BY vehicle_id, step;
    """
    if not depot_nodes:
        raise Exception("cuopt.vrp requires at least one depot node (one vehicle per depot).")

    vertices = [v for v in context.graph.vertices]
    vertex_positions = [
        {LATITUDE: v.properties.get(LATITUDE), LONGITUDE: v.properties.get(LONGITUDE)} for v in vertices
    ]
    cost_matrix = create_distance_matrix(vertex_positions)
    if cost_matrix is None:
        raise Exception("cuopt.vrp requires all nodes to have valid 'lat'/'lng' properties.")

    index_of = {v: i for i, v in enumerate(vertices)}
    try:
        depot_indices = [index_of[d] for d in depot_nodes]
    except KeyError:
        raise Exception("cuopt.vrp: every depot node must also be a node in the queried graph.") from None

    depot_index_set = set(depot_indices)
    task_indices = [i for i in range(len(vertices)) if i not in depot_index_set]
    if not task_indices:
        raise Exception("cuopt.vrp requires at least one non-depot node to visit.")

    capacities = config.get("capacities")
    demands = None
    if capacities is not None:
        demand_property = config.get("demand_property")
        if demand_property is None:
            raise Exception("cuopt.vrp: config.demand_property is required when config.capacities is set.")
        demands = [[vertices[i].properties.get(demand_property, 0) for i in task_indices]]
        capacities = [list(capacities)]

    request = build_vrp_request(
        cost_matrix=cost_matrix,
        vehicle_start_indices=depot_indices,
        task_indices=task_indices,
        capacities=capacities,
        demands=demands,
    )

    host = config.get("host", os.environ.get("CUOPT_SERVER_HOST", _DEFAULT_HOST))
    port = int(config.get("port", os.environ.get("CUOPT_SERVER_PORT", _DEFAULT_PORT)))
    repoll_tries = config.get("repoll_tries", 50)
    repoll_interval_seconds = config.get("repoll_interval_seconds", 1.0)

    try:
        client = CuOptClient(host, port)
        solution = client.solve_vrp(request, repoll_tries=repoll_tries, repoll_interval_seconds=repoll_interval_seconds)
        steps = parse_vrp_response(solution)
    except CuOptConnectionError as e:
        raise Exception(f"cuopt.vrp: could not reach cuopt-server at {host}:{port}: {e}") from None
    except CuOptSolverException as e:
        raise Exception(f"cuopt.vrp: solver failed: {e}") from None

    return [
        mgp.Record(
            vehicle_id=s.vehicle_id,
            step=s.step,
            from_vertex=vertices[s.from_index],
            to_vertex=vertices[s.to_index],
        )
        for s in steps
    ]
