import numpy as np
import pytest
from mage.constraint_programming.vrp_cp_solver import VRPConstraintProgrammingSolver
from mage.geography import InvalidDepotException


@pytest.fixture
def default_distance_matrix():
    return np.array([[0, 1, 2], [1, 0, 3], [2, 3, 0]])


def test_negative_depot_index_raise_exception(default_distance_matrix):
    with pytest.raises(InvalidDepotException):
        VRPConstraintProgrammingSolver(no_vehicles=2, distance_matrix=default_distance_matrix, depot_index=-1)


def test_depot_index_to_big_raise_exception(default_distance_matrix):
    with pytest.raises(InvalidDepotException):
        VRPConstraintProgrammingSolver(
            no_vehicles=2,
            distance_matrix=default_distance_matrix,
            depot_index=len(default_distance_matrix),
        )
