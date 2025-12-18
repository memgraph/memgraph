from typing import Any, Dict, List, Optional

from mage.graph_coloring_module.components.correlation_population import (
  CorrelationPopulation,
)
from mage.graph_coloring_module.components.individual import Individual
from mage.graph_coloring_module.components.population import Population
from mage.graph_coloring_module.graph import Graph
from mage.graph_coloring_module.parameters import Parameter
from mage.graph_coloring_module.utils.generate_individuals import generate_individuals
from mage.graph_coloring_module.utils.parameters_utils import param_value
from mage.graph_coloring_module.utils.validation import validate


class ChainChunkFactory:
    @staticmethod
    @validate(Parameter.POPULATION_SIZE, Parameter.NO_OF_PROCESSES)
    def create(graph: Graph, parameters: Dict[str, Any] = None) -> Optional[List[Population]]:
        """Returns a list of no_of_processes populations that have approximately
        population_size / no_of_processes individuals."""

        population_size = param_value(graph, parameters, Parameter.POPULATION_SIZE)
        no_of_processes = param_value(graph, parameters, Parameter.NO_OF_PROCESSES)

        individuals = generate_individuals(graph, parameters)
        populations = []
        chunks = ChainChunkFactory._list_chunks(individuals, population_size, no_of_processes)

        for i, chunk in enumerate(chunks):
            prev_chunk = i - 1 if i - 1 > 0 else no_of_processes - 1
            next_chunk = i + 1 if i + 1 < no_of_processes else 0
            populations.append(ChainChunk(graph, chunk, chunks[prev_chunk][-1], chunks[next_chunk][0]))
        return populations

    @staticmethod
    def _list_chunks(individuals: List[Individual], population_size: int, no_of_chunks: int) -> List[List[Individual]]:
        """Splits a list into equal parts."""
        k, m = divmod(population_size, no_of_chunks)
        chunks = [list(individuals[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)]) for i in range(no_of_chunks)]
        return chunks


class ChainChunk(CorrelationPopulation):
    """A class that represents a population that is just a part
    of the whole population. First and last individuals of this
    population exchange information with individuals located in
    other parts of the entire population. Pieces of the population
    are ordered. The first individual communicates with the last in
    the previous piece of the population, and the last communicates
    with the first in the next piece of the population."""

    def __init__(
        self,
        graph: Graph,
        individuals: List[Individual],
        prev_indv: Individual,
        next_indv: Individual,
    ):
        super().__init__(graph, individuals)
        self._prev_indv = prev_indv
        self._next_indv = next_indv
        self._set_correlations()

    def _get_prev_correlation_index(self, index: int) -> int:
        """Returns the index of the correlation with the
        previous individual in the chain of individuals."""
        return index - 1 if index - 1 >= 0 else self.size

    def _get_next_correlation_index(Self, index: int) -> int:
        """Returns the index of the correlation with the
        next individual in the chain of individuals."""
        return index

    def get_prev_individual(self, index: int) -> Individual:
        """Returns the individual that precedes the
        individual on the given index."""
        if index < 0 or index >= self.size:
            raise IndexError()
        if index == 0:
            return self._prev_indv
        return self.individuals[index - 1]

    def get_next_individual(self, index: int) -> Individual:
        """Returns the individual that follows the
        individual on the given index."""
        if index < 0 or index >= self.size:
            raise IndexError()
        if index + 1 == self.size:
            return self._next_indv
        return self.individuals[index + 1]

    def _set_correlations(self) -> None:
        for i in range(self.size + 1):
            if i == self.size:
                c = self._calculate_correlation(self.individuals[0], self._prev_indv)
            else:
                next_indv = self.individuals[i + 1] if i + 1 < self.size else self._next_indv
                c = self._calculate_correlation(self.individuals[i], next_indv)
            self._correlation.append(c)
            self._cumulative_correlation += c

    def set_prev_individual(self, individual: Individual) -> None:
        """Sets the unit that precedes the current piece of chain."""
        self._cumulative_correlation -= self._correlation[self.size]
        self._correlation[self.size] = self._calculate_correlation(individual, self.individuals[0])
        self._cumulative_correlation += self._correlation[self.size]
        self._prev_indv = individual

    def set_next_individual(self, individual: Individual) -> None:
        """Sets the individual that follows the current piece of chain."""
        self._cumulative_correlation -= self._correlation[self.size - 1]
        self._correlation[self.size - 1] = self._calculate_correlation(self.individuals[self.size - 1], individual)
        self._cumulative_correlation += self._correlation[self.size - 1]
        self._next_indv = individual
