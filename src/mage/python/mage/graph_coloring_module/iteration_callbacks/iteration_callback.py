from abc import ABC, abstractmethod
from typing import Any, Dict

from mage.graph_coloring_module.components.population import Population
from mage.graph_coloring_module.graph import Graph


class IterationCallback(ABC):
    """
    An abstract class that defines the iteration callback interface.
    Iteration callback is called after each iteration of the iterative
    algorithm. Iteration callback saves certain population information
    depending on the specific implementation. Also, if the conditions
    defined by the implementation are met, certain actions can be called.
    (If convergence is detected, tunneling can be performed.) When the
    algorithm completes execution, the end function should be called and
    the stored information can be processed if needed
    (draw an error graph, etc. )
    """

    @abstractmethod
    def update(self, graph: Graph, population: Population, parameters: Dict[str, Any]):
        pass

    @abstractmethod
    def end(self, graph: Graph, population: Population, parameters: Dict[str, Any]):
        pass
