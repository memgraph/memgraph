from abc import abstractmethod
from typing import List, Tuple

from mage.graph_coloring_module.components.individual import Individual
from mage.graph_coloring_module.components.population import Population
from mage.graph_coloring_module.graph import Graph


class CorrelationPopulation(Population):
    def __init__(self, graph: Graph, individuals: List[Individual]):
        super().__init__(graph, individuals)
        self._cumulative_correlation = 0
        self._correlation = []

    @abstractmethod
    def _set_correlations(self) -> None:
        """Calculates the correlations between individuals
        and stores them in correlation list."""
        pass

    @abstractmethod
    def _get_prev_correlation_index(self, index: int) -> int:
        """Returns the index of the correlation between an individual
        on the given index and the previous individual in the chain of individuals."""
        pass

    @abstractmethod
    def _get_next_correlation_index(self, index: int) -> int:
        """Returns the index of the correlation between an individual
        on the given index and the next individual in the chain of individuals."""
        pass

    def set_individual(self, index: int, individual: Individual, diff_nodes: List[int]) -> None:
        """Sets the individual on the specified index to the given individual
        and updates appropriate correlations and metrics."""
        old_individual = self._individuals[index]
        self._individuals[index] = individual
        self._update_correlation(index, old_individual, diff_nodes)
        self._update_metrics(index, old_individual)

    @property
    def correlation(self) -> int:
        """Returns a list that contains correlations between individuals.
        Correlation on the index i is the correlation between the individual
        placed on the index i in the list of individuals and the individual
        that is next to that individual."""
        return self._correlation

    @property
    def cumulative_correlation(self) -> int:
        """Returns the cumulative correlation of the population."""
        return self._cumulative_correlation

    @property
    def correlations(self, index: int) -> Tuple[int, int]:
        """Returns correlations between a given individual
        and the previous and next individual."""
        prev_index = self._get_prev_correlation_index(index)
        next_index = self._get_next_correlation_index(index)
        return self._correlation[prev_index], self._correlation[next_index]

    def _calculate_correlation(self, first: Individual, second: Individual) -> float:
        correlation = 0
        for node_1 in self._graph.nodes:
            for node_2 in range(node_1 + 1, len(self._graph)):
                S_first = -1 if first[node_1] == first[node_2] else 1
                S_second = -1 if second[node_1] == second[node_2] else 1
                correlation += S_first * S_second
        return correlation

    def _update_correlation(self, index: int, old_individual: Individual, nodes: List[int]) -> int:
        next_correlation_index = self._get_next_correlation_index(index)
        prev_correlation_index = self._get_prev_correlation_index(index)

        new_individual = self.individuals[index]
        prev_individual = self.get_prev_individual(index)
        next_individual = self.get_next_individual(index)

        correlation_prev_delta = 0
        correlation_next_delta = 0
        processed = [False for _ in range(old_individual.no_of_units)]

        for node in nodes:
            for neigh in self._graph[node]:
                if not processed[neigh]:
                    S_old = -1 if old_individual[node] == old_individual[neigh] else 1
                    S_new = -1 if new_individual[node] == new_individual[neigh] else 1
                    S_prev = -1 if prev_individual[node] == prev_individual[neigh] else 1
                    S_next = -1 if next_individual[node] == next_individual[neigh] else 1
                    correlation_prev_delta += (S_new * S_prev) - (S_old * S_prev)
                    correlation_next_delta += (S_new * S_next) - (S_old * S_next)
            processed[node] = True

        self._correlation[prev_correlation_index] += correlation_prev_delta
        self._correlation[next_correlation_index] += correlation_next_delta
        delta_corr = correlation_prev_delta + correlation_next_delta
        self._cumulative_correlation += delta_corr
        return delta_corr
