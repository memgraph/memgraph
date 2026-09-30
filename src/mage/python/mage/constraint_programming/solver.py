import abc
import random
import sys

try:
    from gekko import GEKKO
except ImportError as import_error:
    sys.stderr.write(
        (f"NOTE: Please install gekko in order to be " f"able to use set-cover solver. Using Python: {sys.version}")
    )
    raise import_error


class MatchingProblem(abc.ABC):
    """
    Definition for matching problem of set cover
    """


class GreedyMatchingProblem(MatchingProblem):
    """
    Matching problem to be used with greedy solving of set cover.
    """

    def __init__(self, elements, containing_sets, elements_by_sets):
        self.elements = elements
        self.containing_sets = containing_sets
        self.elements_by_sets = elements_by_sets


class GekkoMatchingProblem(MatchingProblem):
    """
    Matching problem to be used with gekko constraint programming solving of set cover.
    """

    def __init__(self, containing_sets, sets_by_elements):
        self.containing_sets = containing_sets
        self.sets_by_elements = sets_by_elements


class MatchingProblemSolver(abc.ABC):
    """
    Solver of set cover matching problem
    """

    @abc.abstractmethod
    def solve(self, matching_problem: MatchingProblem):
        """
        Solves the matching problem and returns the set indices
        :param matching_problem: matching problem
        :return: set indices
        """
        pass


class GekkoMPSolver(MatchingProblemSolver):
    """
    Solver of set cover with gekko constraint programming
    """

    def solve(self, matching_problem: GekkoMatchingProblem):
        """
        Solves the matching problem and returns the set indices
        :param matching_problem: matching problem
        :return: set indices
        """

        m = GEKKO(remote=False)
        m.options.SOLVER = 1
        containing_const = m.Const(1, name="const")

        set_list = list(matching_problem.containing_sets)
        vars = [m.Var(lb=0, ub=1, integer=True, name=GekkoMPSolver.get_variable_name(i)) for i in range(len(set_list))]

        set_ordinal_map = {value: i for i, value in enumerate(set_list)}

        for element in matching_problem.sets_by_elements.keys():
            containing_sets = matching_problem.sets_by_elements[element]
            contained_sets_eq = 0

            for contained_set in containing_sets:
                ordinal_number = set_ordinal_map[contained_set]
                contained_sets_eq = contained_sets_eq + vars[ordinal_number]

            m.Equation(equation=contained_sets_eq >= containing_const)

        m.Obj(sum(vars))
        m.solve()

        resulting_sets = []
        for idx, var in enumerate(vars):
            if var.value[0] == 1.0:
                resulting_sets.append(set_list[idx])

        return resulting_sets

    @staticmethod
    def get_variable_name(set_no):
        """
        Returns unique variable name based on the set id
        :param set_no: set id
        :return: set variable name
        """

        return f"containing_set_{set_no}"


class GreedyMPSolver(MatchingProblemSolver):
    """
    Solver of set cover with greedy method
    """

    def solve(self, matching_problem: GreedyMatchingProblem):
        """
        Solves the matching problem and returns the set indices
        :param matching_problem: matching problem
        :return: set indices
        """

        universe = matching_problem.elements

        possible_sets = list(matching_problem.containing_sets)
        random.shuffle(possible_sets)

        picked_sets = list()
        covered_universe = set()

        while len(universe) != len(covered_universe):
            picked_set = possible_sets[0]
            picked_sets.append(picked_set)
            possible_sets = possible_sets[1:]

            covered_universe |= matching_problem.elements_by_sets[picked_set]

        return picked_sets
