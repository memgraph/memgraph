import random
from typing import Any, Dict

from mage.graph_coloring_module.algorithms.algorithm import Algorithm
from mage.graph_coloring_module.components.individual import Individual
from mage.graph_coloring_module.graph import Graph
from mage.graph_coloring_module.parameters import Parameter
from mage.graph_coloring_module.utils.available_colors import available_colors
from mage.graph_coloring_module.utils.parameters_utils import param_value
from mage.graph_coloring_module.utils.validation import validate


class LDO(Algorithm):
    """A class that represents the LDO greedy algorithm. This algorithm sorts nodes
    considering their degrees and then colors them sequentially. If it is not possible
    to uniquely determine the color, color is chosen randomly. If coloring the node with
    any possible color would cause conflicts, then the color is chosen randomly."""

    def __str__(self):
        return "LDO"

    @validate(Parameter.NO_OF_COLORS)
    def run(self, graph: Graph, parameters: Dict[str, Any] = None) -> Individual:
        """Returns the individual that represents the result of the LDO algorithm."""

        no_of_colors = param_value(graph, parameters, Parameter.NO_OF_COLORS)

        chromosome = [-1 for _ in graph.nodes]
        sorted_nodes = sorted(list(graph.nodes), key=lambda node: graph.degree(node), reverse=True)

        for node in sorted_nodes:
            colors = available_colors(graph, no_of_colors, chromosome, node)
            if len(colors) > 0:
                color = random.choice(colors)
            else:
                color = random.randint(0, no_of_colors - 1)
            chromosome[node] = color

        return Individual(no_of_colors, graph, chromosome)
