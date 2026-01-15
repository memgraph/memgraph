from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from mage.graph_coloring_module.components.individual import Individual
from mage.graph_coloring_module.graph import Graph


class Algorithm(ABC):
    """An abstract class that represents an algorithm."""

    @abstractmethod
    def run(self, graph: Graph, parameters: Dict[str, Any]) -> Optional[Individual]:
        """Runs the algorithm and returns the best individual."""
        pass
