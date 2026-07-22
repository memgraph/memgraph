from typing import Any, Dict

from mage.graph_coloring_module.components.population import Population
from mage.graph_coloring_module.graph import Graph
from mage.graph_coloring_module.iteration_callbacks.iteration_callback import IterationCallback


class MatplotlibCallback(IterationCallback):
    def __init__(self):
        super().__init__()

    def update(self, graph: Graph, population: Population, parameters: Dict[str, Any]):
        pass

    def end(self, graph: Graph, population: Population, parameters: Dict[str, Any]):
        pass
