import random
from typing import Any, Dict, List, Tuple

from mage.graph_coloring_module.components.individual import Individual
from mage.graph_coloring_module.graph import Graph
from mage.graph_coloring_module.operators.mutations.mutation import Mutation


class MISMutation(Mutation):
    """A class that represents the maximal independent set mutation.
    This mutation finds one maximal independent set and changes
    colors of all nodes in the set to the same color."""

    def __str__(self):
        return "MISMutation"

    def mutate(
        self, graph: Graph, individual: Individual, parameters: Dict[str, Any] = None
    ) -> Tuple[Individual, List[int]]:
        """A function that mutates the given individual and
        returns the new individual and nodes that were changed."""

        maximal_independent_set = self._MIS(graph)
        if len(maximal_independent_set) > 0:
            color = individual[maximal_independent_set[0]]
            colors = [color for _ in range(len(maximal_independent_set))]
            mutated_individual = individual.replace_units(maximal_independent_set, colors)
            return mutated_individual, maximal_independent_set
        return individual, []

    def _MIS(self, graph: Graph) -> List[int]:
        """A function that finds the maximal independent set. The first step
        is to shuffle nodes and add the first node to the maximal independent set.
        After that, all those nodes that do not have neighbors in the MIS are
        sequentially added to the MIS. ."""

        nodes = list(graph.nodes)
        random.shuffle(nodes)
        MIS_flags = [False for _ in range(len(graph))]
        MIS = []

        for node in nodes:
            include = True
            for neigh in graph[node]:
                if MIS_flags[neigh]:
                    include = False
                    break
            if include:
                MIS.append(node)
                MIS_flags[node] = True

        return MIS
