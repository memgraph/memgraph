from abc import ABC, abstractmethod
from typing import Any, Dict

from mage.graph_coloring_module.components.individual import Individual
from mage.graph_coloring_module.components.population import Population
from mage.graph_coloring_module.graph import Graph


class Error(ABC):
    """A class that represents an error function."""

    @abstractmethod
    def individual_err(self, graph: Graph, individual: Individual, parameters: Dict[str, Any] = None) -> float:
        """Calculates the error of the individual."""
        pass

    @abstractmethod
    def population_err(self, graph: Graph, population: Population, parameters: Dict[str, Any] = None) -> float:
        """Calculates the population error."""
        pass
