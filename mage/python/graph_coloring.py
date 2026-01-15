from collections import defaultdict
from typing import Any, Dict, Optional

import mage.graph_coloring_module
import mgp
from mage.graph_coloring_module import Graph, IncorrectParametersException, Parameter


@mgp.read_proc
def color_graph(
    context: mgp.ProcCtx, parameters: mgp.Map = {}, edge_property: str = "weight"
) -> mgp.Record(node=mgp.Vertex, color=int):
    """
    Example:
    CALL graph_coloring.color_graph() YIELD *;
    """
    parameters = _get_parameters(parameters)
    graph = _convert_to_graph(context, edge_property)
    algorithm = parameters[Parameter.ALGORITHM]
    solution = algorithm.run(graph, parameters)
    return [
        mgp.Record(node=context.graph.get_vertex_by_id(graph.label(node)), color=color)
        for node, color in enumerate(solution.chromosome)
    ]


@mgp.read_proc
def color_subgraph(
    context: mgp.ProcCtx,
    vertices: mgp.List[mgp.Vertex],
    edges: mgp.List[mgp.Edge],
    parameters: mgp.Map = {},
    edge_property: str = "weight",
) -> mgp.Record(node=mgp.Vertex, color=int):
    """
    Example:
    MATCH (a:Cell)-[e:CLOSE_TO]->(b:Cell)
    WITH collect(a) as nodes, collect (e) as edges
    CALL graph_coloring.color_subgraph(nodes, edges, {no_of_colors: 2})
    YIELD color, node
    RETURN color, node;
    """
    parameters = _get_parameters(parameters)
    graph = _convert_to_subgraph(context, vertices, edges, edge_property)
    algorithm = parameters[Parameter.ALGORITHM]
    solution = algorithm.run(graph, parameters)
    return [
        mgp.Record(node=context.graph.get_vertex_by_id(graph.label(node)), color=color)
        for node, color in enumerate(solution.chromosome)
    ]


def _str2Class(name: str):
    if name not in dir(mage.graph_coloring_module):
        raise IncorrectParametersException(f"Parameter {name} is incorrect.")
    return getattr(mage.graph_coloring_module, name)


def _map_parameters(parameters: Dict[str, Any]) -> Dict[str, Any]:
    for key in parameters:
        if isinstance(parameters[key], str):
            parameters[key] = _str2Class(parameters[key])()
        if isinstance(parameters[key], tuple) or isinstance(parameters[key], list):
            new_list = []
            for val in parameters[key]:
                if isinstance(val, str):
                    new_list.append(_str2Class(val)())
                else:
                    new_list.append(val)
            parameters[key] = new_list
    return parameters


def _get_parameters(parameters: Dict[str, Any]) -> Dict[str, Any]:
    params = _map_parameters(
        {
            Parameter.ALGORITHM: parameters.get(Parameter.ALGORITHM.value, "QA"),
            Parameter.NO_OF_COLORS: parameters.get(Parameter.NO_OF_COLORS.value, 10),
            Parameter.NO_OF_PROCESSES: parameters.get(Parameter.NO_OF_PROCESSES.value, 1),
            Parameter.POPULATION_SIZE: parameters.get(Parameter.POPULATION_SIZE.value, 15),
            Parameter.INIT_ALGORITHMS: parameters.get(Parameter.INIT_ALGORITHMS.value, ["SDO", "LDO"]),
            Parameter.POPULATION_FACTORY: parameters.get(Parameter.POPULATION_FACTORY.value, "ChainChunkFactory"),
            Parameter.ERROR: parameters.get(Parameter.ERROR.value, "ConflictError"),
            Parameter.MAX_ITERATIONS: parameters.get(Parameter.MAX_ITERATIONS.value, 10),
            Parameter.ITERATION_CALLBACKS: parameters.get(Parameter.ITERATION_CALLBACKS.value, []),
            Parameter.COMMUNICATION_DALAY: parameters.get(Parameter.COMMUNICATION_DALAY.value, 10),
            Parameter.LOGGING_DELAY: parameters.get(Parameter.LOGGING_DELAY.value, 10),
            Parameter.QA_TEMPERATURE: parameters.get(Parameter.QA_TEMPERATURE.value, 0.035),
            Parameter.QA_MAX_STEPS: parameters.get(Parameter.QA_MAX_STEPS.value, 10),
            Parameter.CONFLICT_ERR_ALPHA: parameters.get(Parameter.CONFLICT_ERR_ALPHA.value, 0.1),
            Parameter.CONFLICT_ERR_BETA: parameters.get(Parameter.CONFLICT_ERR_BETA.value, 0.001),
            Parameter.MUTATION: parameters.get(Parameter.MUTATION.value, "SimpleMutation"),
            Parameter.MULTIPLE_MUTATION_NODES_NO_OF_NODES: parameters.get(
                Parameter.MULTIPLE_MUTATION_NODES_NO_OF_NODES.value, 2
            ),
            Parameter.RANDOM_MUTATION_PROBABILITY: parameters.get(Parameter.RANDOM_MUTATION_PROBABILITY.value, 0.1),
            Parameter.SIMPLE_TUNNELING_MUTATION: parameters.get(
                Parameter.SIMPLE_TUNNELING_MUTATION.value, "MultipleMutation"
            ),
            Parameter.SIMPLE_TUNNELING_PROBABILITY: parameters.get(Parameter.SIMPLE_TUNNELING_PROBABILITY.value, 0.5),
            Parameter.SIMPLE_TUNNELING_ERROR_CORRECTION: parameters.get(
                Parameter.SIMPLE_TUNNELING_ERROR_CORRECTION.value, 2
            ),
            Parameter.SIMPLE_TUNNELING_MAX_ATTEMPTS: parameters.get(Parameter.SIMPLE_TUNNELING_MAX_ATTEMPTS.value, 25),
            Parameter.CONVERGENCE_CALLBACK_TOLERANCE: parameters.get(
                Parameter.CONVERGENCE_CALLBACK_TOLERANCE.value, 500
            ),
            Parameter.CONVERGENCE_CALLBACK_ACTIONS: parameters.get(
                Parameter.CONVERGENCE_CALLBACK_ACTIONS.value, ["SimpleTunneling"]
            ),
        }
    )

    return params


def _convert_to_graph(context: mgp.ProcCtx, edge_property: str) -> Graph:
    nodes = []
    adj_list = defaultdict(list)

    for v in context.graph.vertices:
        context.check_must_abort()
        nodes.append(v.id)

    for v in context.graph.vertices:
        context.check_must_abort()
        for e in v.out_edges:
            weight = e.properties.get(edge_property, 1)
            adj_list[e.from_vertex.id].append((e.to_vertex.id, weight))
            adj_list[e.to_vertex.id].append((e.from_vertex.id, weight))

    return Graph(nodes, adj_list)


def _convert_to_subgraph(
    context: mgp.ProcCtx,
    vertices: mgp.List[mgp.Vertex],
    edges: mgp.List[mgp.Edge],
    edge_property: str,
) -> Optional[Graph]:
    vertices, edges = map(set, [vertices, edges])

    nodes = []
    adj_list = defaultdict(list)

    for v in vertices:
        context.check_must_abort()
        nodes.append(v.id)

    for e in edges:
        context.check_must_abort()
        weight = e.properties.get(edge_property, 1)
        if e.from_vertex.id not in nodes:
            nodes.append(e.from_vertex.id)
        if e.to_vertex.id not in nodes:
            nodes.append(e.to_vertex.id)
        adj_list[e.from_vertex.id].append((e.to_vertex.id, weight))
        adj_list[e.to_vertex.id].append((e.from_vertex.id, weight))

    return Graph(nodes, adj_list)
