import abc
from collections import defaultdict
from typing import List

import mgp
from mage.constraint_programming import (
  GekkoMatchingProblem,
  GekkoMPSolver,
  GreedyMatchingProblem,
  GreedyMPSolver,
)


@mgp.read_proc
def cp_solve(
    context: mgp.ProcCtx,
    element_vertexes: List[mgp.Vertex],
    set_vertexes: List[mgp.Vertex],
) -> mgp.Record(containing_set=mgp.Vertex):
    """
    This set cover solver method returns 1 filed

      * `containing_set` is a minimal set of sets in which all the element have been contained

    The input arguments consist of

      * `element_vertexes` that is a list of element nodes
      * `set_vertexes` that is a list of set nodes those elements are contained in

    Element and set equivalents at a certain index come in pairs so mappings between sets and elements are consistent.

    The procedure can be invoked in openCypher using the following calls, e.g.:
      CALL set_cover.cp_solve([(:Point), (:Point)], [(:Set), (:Set)]) YIELD containing_set;

    The method uses constraint programming as a solving tool for obtaining a minimal set of sets that contain
        all the elements.
    """

    creator = GekkoMatchingProblemCreator()
    mp = creator.create_matching_problem(element_vertexes, set_vertexes)

    solver = GekkoMPSolver()
    result = solver.solve(matching_problem=mp)

    resulting_nodes = [context.graph.get_vertex_by_id(x) for x in result]

    return [mgp.Record(containing_set=x) for x in resulting_nodes]


@mgp.read_proc
def greedy(
    context: mgp.ProcCtx,
    element_vertexes: List[mgp.Vertex],
    set_vertexes: List[mgp.Vertex],
) -> mgp.Record(containing_set=mgp.Vertex):
    """
    This set cover solver method returns 1 filed

      * `containing_set` is a minimal set of sets in which all the element have been contained

    The input arguments consist of

      * `element_vertexes` that is a list of element nodes
      * `set_vertexes` that is a list of set nodes those elements are contained in

    Element and set equivalents at a certain index come in pairs so mappings between sets and elements are consistent.

    The procedure can be invoked in openCypher using the following calls, e.g.:
      CALL set_cover.greedy([(:Point), (:Point)], [(:Set), (:Set)]) YIELD containing_set;

    The method uses a greedy method as a solving tool for obtaining a minimal set of sets that contain
        all the elements.
    """

    creator = GreedyMatchingProblemCreator()
    mp = creator.create_matching_problem(element_vertexes, set_vertexes)

    solver = GreedyMPSolver()
    result = solver.solve(matching_problem=mp)

    resulting_nodes = [context.graph.get_vertex_by_id(x) for x in result]

    return [mgp.Record(containing_set=x) for x in resulting_nodes]


class MatchingProblemCreator(abc.ABC):
    """
    Creator abstract class of matching problems
    """

    @abc.abstractmethod
    def create_matching_problem(self, element_vertexes, set_vertexes):
        """
        Creates a matching problem
        :param element_vertexes: Element vertexes pair component list
        :param set_vertexes: Set vertexes pair component list
        :return: matching problem
        """

        pass


class GreedyMatchingProblemCreator(MatchingProblemCreator):
    """
    Creator class for set cover to be solved with greedy method
    """

    def create_matching_problem(self, element_vertexes: List[mgp.Vertex], set_vertexes: List[mgp.Vertex]):
        """
        Creates a matching problem to be solved with greedy method
        :param element_vertexes: Element vertexes pair component list
        :param set_vertexes: Set vertexes pair component list
        :return: matching problem
        """

        element_values = [x.id for x in element_vertexes]
        set_values = [x.id for x in set_vertexes]
        all_elements = set(element_values)
        all_sets = set(set_values)

        elements_by_sets = defaultdict(set)

        for element, contained_set in zip(element_values, set_values):
            elements_by_sets[contained_set].add(element)

        return GreedyMatchingProblem(all_elements, all_sets, elements_by_sets)


class GekkoMatchingProblemCreator(MatchingProblemCreator):
    """
    Creator class for set cover to be solved with gekko constraint programming
    """

    def create_matching_problem(self, element_vertexes: List[mgp.Vertex], set_vertexes: List[mgp.Vertex]):
        """
        Creates a matching problem to be solved with gekko constraint programming method
        :param element_vertexes: Element vertexes pair component list
        :param set_vertexes: Set vertexes pair component list
        :return: matching problem
        """

        element_values = [x.id for x in element_vertexes]
        set_values = [x.id for x in set_vertexes]
        set_values_distinct = set(set_values)
        sets_by_elements = defaultdict(set)

        for element, contained_set in zip(element_values, set_values):
            sets_by_elements[element].add(contained_set)

        return GekkoMatchingProblem(set_values_distinct, sets_by_elements)
