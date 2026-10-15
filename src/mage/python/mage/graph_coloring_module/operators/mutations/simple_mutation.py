import random
from typing import Any, Dict, List, Optional, Tuple

from mage.graph_coloring_module.components.individual import Individual
from mage.graph_coloring_module.graph import Graph
from mage.graph_coloring_module.operators.mutations.mutation import Mutation


class SimpleMutation(Mutation):
    """A class that represents the Simple Mutation. This mutation
    randomly chooses one node in a graph that is involved in a conflict
    and changes its color to a new randomly selected color."""

    def __str__(self):
        return "SimpleMutation"

    def mutate(
        self, graph: Graph, individual: Individual, parameters: Dict[str, Any] = None
    ) -> Optional[Tuple[Individual, List[int]]]:
        """A function that mutates the given individual and
        returns the new individual and nodes that were changed."""
        conflict_nodes = individual.conflict_nodes
        if len(conflict_nodes) == 0:
            return individual, []
        node = random.choice(list(conflict_nodes))
        color = random.randint(0, individual.no_of_colors - 1)
        mutated_indv = individual.replace_unit(node, color)
        return mutated_indv, [node]
