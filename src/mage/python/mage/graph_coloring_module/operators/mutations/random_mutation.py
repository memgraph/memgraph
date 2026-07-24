import random
from typing import Any, Dict, List, Tuple

from mage.graph_coloring_module.components.individual import Individual
from mage.graph_coloring_module.graph import Graph
from mage.graph_coloring_module.operators.mutations.mutation import Mutation
from mage.graph_coloring_module.parameters import Parameter
from mage.graph_coloring_module.utils.available_colors import available_colors
from mage.graph_coloring_module.utils.parameters_utils import param_value
from mage.graph_coloring_module.utils.validation import validate


class RandomMutation(Mutation):
    """A class that represents the Random Mutation. This mutation
    changes the color of exactly one selected node. The selected node
    can be a random node in the graph or a node that is in conflict.
    The probability that a random node is selected is given as a parameter
    named random_mutation_probability. If a random node is not selected, then
    one of the conflicting nodes is selected."""

    def __str__(self):
        return "RandomMutation"

    @validate(Parameter.RANDOM_MUTATION_PROBABILITY, Parameter.NO_OF_COLORS)
    def mutate(
        self, graph: Graph, individual: Individual, parameters: Dict[str, Any] = None
    ) -> Tuple[Individual, List[int]]:
        """A function that mutates the given individual and
        returns the new individual and nodes that were changed."""

        random_mutation_probability = param_value(graph, parameters, Parameter.RANDOM_MUTATION_PROBABILITY)
        no_of_colors = param_value(graph, parameters, Parameter.NO_OF_COLORS)

        conflict_nodes = individual.conflict_nodes
        non_conflict_nodes = []
        for node in graph.nodes:
            if node not in conflict_nodes:
                non_conflict_nodes.append(node)

        if len(conflict_nodes) == 0:
            return individual, []

        node = (
            random.choice(non_conflict_nodes)
            if len(non_conflict_nodes) > 0 and random.random() < random_mutation_probability
            else random.choice(list(conflict_nodes))
        )
        colors = available_colors(graph, no_of_colors, individual.chromosome, node)
        color = random.choice(colors) if len(colors) > 0 else random.randint(0, no_of_colors - 1)

        mutated_individual = individual.replace_unit(node, color)
        return mutated_individual, [node]
