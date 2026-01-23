from typing import Any, Dict

from mage.graph_coloring_module.algorithms.algorithm import Algorithm
from mage.graph_coloring_module.components.individual import Individual
from mage.graph_coloring_module.graph import Graph
from mage.graph_coloring_module.parameters import Parameter
from mage.graph_coloring_module.utils.parameters_utils import param_value
from mage.graph_coloring_module.utils.validation import validate


class Random(Algorithm):
    """A class that represents the algorithm that randomly colors nodes."""

    def __str__(self):
        return "Random"

    @validate(Parameter.NO_OF_COLORS)
    def run(self, graph: Graph, parameters: Dict[str, Any] = None) -> Individual:
        no_of_colors = param_value(graph, parameters, Parameter.NO_OF_COLORS)
        return Individual(no_of_colors, graph)
