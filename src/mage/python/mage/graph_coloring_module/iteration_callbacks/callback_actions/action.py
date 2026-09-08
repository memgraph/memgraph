from abc import ABC, abstractmethod
from typing import Any, Dict

from mage.graph_coloring_module.components.population import Population
from mage.graph_coloring_module.graph import Graph


class Action(ABC):
    """An abstract class that defines action. The action defines
    what happens when the conditions specified in the iteration
    callback are met."""

    @abstractmethod
    def execute(self, graph: Graph, population: Population, parameters: Dict[str, Any] = None) -> None:
        pass
