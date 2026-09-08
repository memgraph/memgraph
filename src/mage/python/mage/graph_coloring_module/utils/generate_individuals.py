import logging
from typing import Any, Dict, List

from mage.graph_coloring_module.components.individual import Individual
from mage.graph_coloring_module.exceptions import PopulationCreationException
from mage.graph_coloring_module.graph import Graph
from mage.graph_coloring_module.parameters import Parameter
from mage.graph_coloring_module.utils.parameters_utils import param_value
from mage.graph_coloring_module.utils.validation import validate

logger = logging.getLogger("graph_coloring")


@validate(Parameter.POPULATION_SIZE, Parameter.NO_OF_COLORS)
def generate_individuals(graph: Graph, parameters: Dict[str, Any] = None) -> List[Individual]:
    """A function that creates a list of individuals in which some individuals
    are the results of the given algorithms. If more algorithms are given than
    the population size, then the remainder is ignored. If population creation has failed,
    an exception is raised."""

    population_size = param_value(graph, parameters, Parameter.POPULATION_SIZE)
    no_of_colors = param_value(graph, parameters, Parameter.NO_OF_COLORS)
    algorithms = param_value(graph, parameters, Parameter.INIT_ALGORITHMS)

    individuals = []
    if algorithms is not None:
        for algorithm in algorithms:
            individual = algorithm.run(graph, parameters)
            if individual is None:
                logger.error("Population creation has not succeeded.")
                raise PopulationCreationException("Population creation has not succeeded.")
            if len(individuals) < population_size:
                individuals.append(individual)

    individuals.extend([Individual(no_of_colors, graph) for _ in range(population_size - len(individuals))])
    return individuals
