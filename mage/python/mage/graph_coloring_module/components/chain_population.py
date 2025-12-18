from typing import Any, Dict, List, Optional

from mage.graph_coloring_module.components.correlation_population import (
  CorrelationPopulation,
)
from mage.graph_coloring_module.components.individual import Individual
from mage.graph_coloring_module.components.population import Population
from mage.graph_coloring_module.graph import Graph
from mage.graph_coloring_module.utils.generate_individuals import generate_individuals


class ChainPopulationFactory:
    @staticmethod
    def create(graph: Graph, parameters: Dict[str, Any] = None) -> Optional[List[Population]]:
        individuals = generate_individuals(graph, parameters)
        return [ChainPopulation(graph, individuals)]


class ChainPopulation(CorrelationPopulation):
    """A class that represents a chain population. In this
    population, the last individual is followed by the first
    individual, and the predecessor of the first individual
    is the last individual."""

    def __init__(self, graph: Graph, individuals: List[Individual]):
        super().__init__(graph, individuals)
        self._set_correlations()

    def _get_prev_correlation_index(self, index: int) -> int:
        """Returns the index of the correlation with the previous
        individual in the chain of individuals."""
        return index - 1 if index - 1 >= 0 else self.size - 1

    def _get_next_correlation_index(self, index: int) -> int:
        """Returns the index of the correlation with the next
        individual in the chain of individuals."""
        return index

    def get_prev_individual(self, index: int) -> Individual:
        """Returns the individual that precedes the individual on the given index."""
        if index < 0 or index >= self.size:
            raise IndexError()
        prev_ind = index - 1 if index - 1 >= 0 else self.size - 1
        return self.individuals[prev_ind]

    def get_next_individual(self, index: int) -> Individual:
        """Returns the individual that follows the individual on the given index."""
        if index < 0 or index >= self.size:
            raise IndexError()
        next_ind = index + 1 if index + 1 < self.size else 0
        return self.individuals[next_ind]

    def _set_correlations(self) -> None:
        for i in range(self.size):
            j = i + 1 if i + 1 < self.size else 0
            c = self._calculate_correlation(self.individuals[i], self.individuals[j])
            self._correlation.append(c)
            self._cumulative_correlation += c
