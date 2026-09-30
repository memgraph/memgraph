from abc import ABC, abstractmethod
from collections import namedtuple
from typing import List

"""
VRP Path is an edge from a starting to ending node
"""
VRPPath = namedtuple("VRPPath", ("from_vertex, to_vertex"))


class VRPResult:
    """
    The VRP Result consists of multiple VRP paths.
    """

    def __init__(self, vrp_paths: List[VRPPath]):
        self.vrp_paths = vrp_paths


class VRPSolver(ABC):
    """
    VRP Solver solves the VRP problem and can extract results to desired hook.
    """

    @abstractmethod
    def solve(self):
        """
        Implementation method.
        """
        pass

    @abstractmethod
    def get_result(self):
        """
        Extract results from solved problem.
        """
        pass


class InvalidDepotException(Exception):
    pass
