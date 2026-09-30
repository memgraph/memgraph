import logging
import multiprocessing as mp
from abc import ABC, abstractmethod
from typing import Any, Dict

from mage.graph_coloring_module.algorithms.algorithm import Algorithm
from mage.graph_coloring_module.components.individual import Individual
from mage.graph_coloring_module.components.population import Population
from mage.graph_coloring_module.graph import Graph
from mage.graph_coloring_module.parameters import Parameter
from mage.graph_coloring_module.utils.parameters_utils import param_value
from mage.graph_coloring_module.utils.validation import validate

logger = logging.getLogger("graph_coloring")


class ParallelAlgorithm(Algorithm, ABC):
    """A class that represents an abstract parallel algorithm."""

    @validate(
        Parameter.NO_OF_PROCESSES,
        Parameter.ERROR,
        Parameter.POPULATION_FACTORY,
    )
    def run(self, graph: Graph, parameters: Dict[str, Any]) -> Individual:
        """Runs the algorithm in a given number of processes and returns the best individual.

        Parameters that must be specified:
        :no_of_processes: the number of processes to run an algorithm in
        :error: a function that defines an error"""

        no_of_processes = param_value(graph, parameters, Parameter.NO_OF_PROCESSES)
        error = param_value(graph, parameters, Parameter.ERROR)
        population_factory = param_value(graph, parameters, Parameter.POPULATION_FACTORY)

        populations = population_factory.create(graph, parameters)

        with mp.Manager() as manager:
            running_flag = mp.Value("i", 1)
            best_solutions = manager.dict()
            last_individuals = manager.dict()
            first_individuals = manager.dict()

            for pid in range(no_of_processes):
                best_solutions[pid] = populations[pid].best_individual(error.individual_err)
                if error.individual_err(graph, best_solutions[pid], parameters) < 1e-5:
                    running_flag = 0
                last_individuals[pid] = populations[pid][-1]
                first_individuals[pid] = populations[pid][0]

            processes = [
                mp.Process(
                    target=self.algorithm,
                    args=(
                        pid,
                        graph,
                        populations[pid],
                        best_solutions,
                        first_individuals,
                        last_individuals,
                        running_flag,
                        parameters,
                    ),
                )
                for pid in range(no_of_processes)
            ]

            for p in processes:
                p.start()

            for p in processes:
                p.join()

            best_individual = min(
                best_solutions.values(),
                key=lambda individual: error.individual_err(graph, individual, parameters),
            )

            return best_individual

    @abstractmethod
    def algorithm(
        self,
        pid: int,
        graph: Graph,
        population: Population,
        best_solutions: Dict[int, Individual],
        first_individuals: Dict[int, Individual],
        last_individuals: Dict[int, Individual],
        running_flag: mp.Value,
        parameters: Dict[str, Any],
    ) -> None:
        """A function that executes an algorithm."""
        pass

    def _previous_pid(self, pid: int, no_of_processes: int):
        prev_pid = pid - 1 if pid - 1 > 0 else no_of_processes - 1
        return prev_pid

    def _next_pid(self, pid: int, no_of_processes: int):
        next_pid = pid + 1 if pid + 1 < no_of_processes else 0
        return next_pid
