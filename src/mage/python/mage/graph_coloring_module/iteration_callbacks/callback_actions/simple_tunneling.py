import random
from typing import Any, Dict

from mage.graph_coloring_module.components.population import Population
from mage.graph_coloring_module.graph import Graph
from mage.graph_coloring_module.iteration_callbacks.callback_actions.action import Action
from mage.graph_coloring_module.parameters import Parameter
from mage.graph_coloring_module.utils.parameters_utils import param_value
from mage.graph_coloring_module.utils.validation import validate


class SimpleTunneling(Action):
    """
    A class that represents SimpleTunneling. This action changes
    individuals of a given population. The probability of changing
    an individual is given as a parameter simple_tunneling_probability.
    The parameter simple_tunneling_mutation defines a mutation that changes
    individuals. The mutated individual is accepted only if its error is less
    than the error of the old individual multiplied by the parameter
    simple_tunneling_error_correction. The maximum number of mutation attempts
    until the individual is accepted is defined as a parameter
    simple_tunneling_max_attempts.
    """

    @validate(
        Parameter.SIMPLE_TUNNELING_MUTATION,
        Parameter.SIMPLE_TUNNELING_PROBABILITY,
        Parameter.SIMPLE_TUNNELING_MAX_ATTEMPTS,
        Parameter.SIMPLE_TUNNELING_ERROR_CORRECTION,
    )
    def execute(
        self,
        graph: Graph,
        population: Population,
        parameters: Dict[str, Any] = None,
    ) -> None:
        simple_tunneling_max_attempts = param_value(graph, parameters, Parameter.SIMPLE_TUNNELING_MAX_ATTEMPTS)
        simple_tunneling_mutation = param_value(graph, parameters, Parameter.SIMPLE_TUNNELING_MUTATION)
        simple_tunneling_probability = param_value(graph, parameters, Parameter.SIMPLE_TUNNELING_PROBABILITY)
        simple_tunneling_error_correction = param_value(graph, parameters, Parameter.SIMPLE_TUNNELING_ERROR_CORRECTION)

        for i, old_individual in enumerate(population.individuals):
            if random.random() < simple_tunneling_probability:
                old_individual_error = old_individual.conflicts_weight
                for _ in range(simple_tunneling_max_attempts):
                    new_individual, diff_nodes = simple_tunneling_mutation.mutate(graph, old_individual, parameters)
                    if new_individual.conflicts_weight <= simple_tunneling_error_correction * old_individual_error:
                        population.set_individual(i, new_individual, diff_nodes)
                        break
