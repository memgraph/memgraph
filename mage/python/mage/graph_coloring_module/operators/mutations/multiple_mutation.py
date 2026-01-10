import random
from typing import Any, Dict, List, Tuple

from mage.graph_coloring_module.components.individual import Individual
from mage.graph_coloring_module.graph import Graph
from mage.graph_coloring_module.operators.mutations.mutation import Mutation
from mage.graph_coloring_module.parameters import Parameter
from mage.graph_coloring_module.utils.parameters_utils import param_value
from mage.graph_coloring_module.utils.validation import validate


class MultipleMutation(Mutation):
    """A class that represents the multiple mutation. This mutation
    changes the colors of randomly selected nodes. The color is chosen
    randomly. The number of nodes to which the color needs to be changed
    is given as a mutation parameter."""

    def __str__(self):
        return "MultipleMutation"

    @validate(Parameter.MULTIPLE_MUTATION_NODES_NO_OF_NODES, Parameter.NO_OF_COLORS)
    def mutate(
        self, graph: Graph, individual: Individual, parameters: Dict[str, Any] = None
    ) -> Tuple[Individual, List[int]]:
        """A function that mutates the given individual and returns
        the new individual and nodes that were changed."""

        no_of_nodes_to_mutate = param_value(graph, parameters, Parameter.MULTIPLE_MUTATION_NODES_NO_OF_NODES)
        no_of_colors = param_value(graph, parameters, Parameter.NO_OF_COLORS)

        nodes = [random.randint(0, len(graph) - 1) for _ in range(no_of_nodes_to_mutate)]
        colors = [random.randint(0, no_of_colors - 1) for _ in range(no_of_nodes_to_mutate)]
        mutated_individual = individual.replace_units(nodes, colors)
        return mutated_individual, nodes
