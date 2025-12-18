import logging
import math
import random
from typing import Any, Dict

from mage.graph_coloring_module.algorithms.meta_heuristics.parallel_algorithm import (
  ParallelAlgorithm,
)
from mage.graph_coloring_module.components.individual import Individual
from mage.graph_coloring_module.components.population import Population
from mage.graph_coloring_module.graph import Graph
from mage.graph_coloring_module.parameters import Parameter
from mage.graph_coloring_module.utils.parameters_utils import param_value
from mage.graph_coloring_module.utils.validation import validate

logger = logging.getLogger("graph_coloring")


class QA(ParallelAlgorithm):
    """A class that represents the quantum annealing algorithm."""

    def __str__(self):
        return "QA"

    @validate(
        Parameter.MAX_ITERATIONS,
        Parameter.ERROR,
        Parameter.COMMUNICATION_DALAY,
        Parameter.LOGGING_DELAY,
        Parameter.ITERATION_CALLBACKS,
        Parameter.NO_OF_PROCESSES,
    )
    def algorithm(
        self,
        pid: int,
        graph: Graph,
        population: Population,
        best_solutions: Dict[int, Individual],
        first_individuals: Dict[int, Individual],
        last_individuals: Dict[int, Individual],
        running_flag,
        parameters: Dict[str, Any],
    ) -> None:
        """Function that executes the QA algorithm. The resulting population
        is written to the queue named results."""

        max_iterations = param_value(graph, parameters, Parameter.MAX_ITERATIONS)
        error = param_value(graph, parameters, Parameter.ERROR)
        communication_delay = param_value(graph, parameters, Parameter.COMMUNICATION_DALAY)
        logging_delay = param_value(graph, parameters, Parameter.LOGGING_DELAY)
        iteration_callbacks = param_value(graph, parameters, Parameter.ITERATION_CALLBACKS)
        no_of_processes = param_value(graph, parameters, Parameter.NO_OF_PROCESSES)

        for iteration in range(max_iterations):
            if running_flag == 0:
                return

            for i in range(len(population)):
                self._markow_chain(graph, population, i, parameters)

            best_individual = population.best_individual(error.individual_err)
            if error.individual_err(graph, best_individual, parameters) < error.individual_err(
                graph, best_solutions[pid], parameters
            ):
                best_solutions[pid] = best_individual

            if math.fabs(error.individual_err(graph, best_individual, parameters)) < 1e-5:
                with running_flag.get_lock():
                    running_flag = 0
                return

            if iteration % communication_delay == 0:
                first_individuals[pid] = population[0]
                last_individuals[pid] = population[-1]

                population.set_next_individual(first_individuals[self._next_pid(pid, no_of_processes)])
                population.set_prev_individual(last_individuals[self._previous_pid(pid, no_of_processes)])

            for callback in iteration_callbacks:
                callback.update(graph, population, parameters)

            if iteration % logging_delay == 0:
                logger.info(
                    "Id: {} Iteration: {} Error: {}".format(pid, iteration, population.min_error(error.individual_err))
                )

        logger.info("Id: {} Iteration: {} Error: {}".format(pid, iteration, population.min_error(error.individual_err)))

        for callback in iteration_callbacks:
            callback.end(graph, population, parameters)

        return

    @validate(
        Parameter.QA_TEMPERATURE,
        Parameter.QA_MAX_STEPS,
        Parameter.MUTATION,
        Parameter.ERROR,
    )
    def _markow_chain(
        self,
        graph: Graph,
        population: Population,
        index: int,
        parameters: Dict[str, Any],
    ) -> None:
        temperature = param_value(graph, parameters, Parameter.QA_TEMPERATURE)
        max_steps = param_value(graph, parameters, Parameter.QA_MAX_STEPS)
        mutation = param_value(graph, parameters, Parameter.MUTATION)
        error = param_value(graph, parameters, Parameter.ERROR)

        for _ in range(max_steps):
            individual = population[index]
            population_error_old = error.population_err(graph, population, parameters)
            new_individual, diff_nodes = mutation.mutate(graph, individual, parameters)
            delta_individual_error = error.individual_err(graph, new_individual) - error.individual_err(
                graph, individual
            )
            population.set_individual(index, new_individual, diff_nodes)
            population_error_new = error.population_err(graph, population, parameters)
            delta_population_error = population_error_new - population_error_old

            if delta_individual_error > 0 or delta_population_error > 0:
                try:
                    probability = 1 - math.exp((-1 * delta_population_error) / temperature)
                except OverflowError:
                    probability = 1
                if random.random() <= probability:
                    population.set_individual(index, individual, diff_nodes)
