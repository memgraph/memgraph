import numpy as np
import pytest
from mage.cuopt.client import CuOptSolverException, VRPStep, build_vrp_request, parse_vrp_response


@pytest.fixture
def default_cost_matrix():
    return np.array([[0, 1, 2, 3], [1, 0, 3, 4], [2, 3, 0, 5], [3, 4, 5, 0]])


def test_build_vrp_request_minimal_shape(default_cost_matrix):
    request = build_vrp_request(
        cost_matrix=default_cost_matrix,
        vehicle_start_indices=[0],
        task_indices=[1, 2, 3],
    )

    assert request["cost_matrix_data"]["data"]["0"] == default_cost_matrix.tolist()
    assert request["task_data"]["task_locations"] == [1, 2, 3]
    assert request["fleet_data"]["vehicle_locations"] == [[0, 0]]
    assert "capacities" not in request["fleet_data"]
    assert "demand" not in request["task_data"]


def test_build_vrp_request_multiple_vehicles(default_cost_matrix):
    request = build_vrp_request(
        cost_matrix=default_cost_matrix,
        vehicle_start_indices=[0, 1],
        task_indices=[2, 3],
    )

    assert request["fleet_data"]["vehicle_locations"] == [[0, 0], [1, 1]]
    assert request["task_data"]["task_locations"] == [2, 3]


def test_build_vrp_request_with_capacities_and_demand(default_cost_matrix):
    request = build_vrp_request(
        cost_matrix=default_cost_matrix,
        vehicle_start_indices=[0],
        task_indices=[1, 2, 3],
        capacities=[[10]],
        demands=[[2, 3, 4]],
    )

    assert request["fleet_data"]["capacities"] == [[10]]
    assert request["task_data"]["demand"] == [[2, 3, 4]]


def test_build_vrp_request_no_vehicles_raises(default_cost_matrix):
    with pytest.raises(ValueError):
        build_vrp_request(cost_matrix=default_cost_matrix, vehicle_start_indices=[], task_indices=[1, 2])


def test_build_vrp_request_no_tasks_raises(default_cost_matrix):
    with pytest.raises(ValueError):
        build_vrp_request(cost_matrix=default_cost_matrix, vehicle_start_indices=[0], task_indices=[])


def test_parse_vrp_response_single_vehicle():
    response = {
        "response": {
            "solver_response": {
                "status": 0,
                "vehicle_data": {
                    "0": {
                        "task_id": ["Depot", "0", "1", "Depot"],
                        "type": ["Depot", "Delivery", "Delivery", "Depot"],
                        "route": [0, 1, 2, 0],
                    }
                },
            }
        },
        "reqId": "some-request-id",
    }

    steps = parse_vrp_response(response)

    assert steps == [
        VRPStep(vehicle_id=0, step=0, from_index=0, to_index=1),
        VRPStep(vehicle_id=0, step=1, from_index=1, to_index=2),
        VRPStep(vehicle_id=0, step=2, from_index=2, to_index=0),
    ]


def test_parse_vrp_response_multiple_vehicles():
    response = {
        "response": {
            "solver_response": {
                "status": 0,
                "vehicle_data": {
                    "0": {"route": [0, 1, 0]},
                    "1": {"route": [0, 2, 0]},
                },
            }
        }
    }

    steps = parse_vrp_response(response)

    assert VRPStep(vehicle_id=0, step=0, from_index=0, to_index=1) in steps
    assert VRPStep(vehicle_id=1, step=0, from_index=0, to_index=2) in steps
    assert len(steps) == 4


def test_parse_vrp_response_infeasible_raises():
    response = {"response": {"solver_response": {"status": 1, "solver_infeasible_message": "no feasible solution"}}}

    with pytest.raises(CuOptSolverException):
        parse_vrp_response(response)


def test_parse_vrp_response_still_pending_raises():
    response = {"reqId": "some-request-id"}

    with pytest.raises(CuOptSolverException):
        parse_vrp_response(response)
