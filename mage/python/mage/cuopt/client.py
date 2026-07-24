import time
from collections import namedtuple
from typing import Dict, List, Optional

try:
    from cuopt_sh_client import CuOptServiceSelfHostClient

    HAS_CUOPT_SH_CLIENT = True
except ImportError:
    HAS_CUOPT_SH_CLIENT = False

VRPStep = namedtuple("VRPStep", "vehicle_id step from_index to_index")

CUOPT_SUCCESS_STATUS = 0


class CuOptSolverException(Exception):
    pass


class CuOptConnectionError(Exception):
    pass


def build_vrp_request(
    cost_matrix,
    vehicle_start_indices: List[int],
    task_indices: List[int],
    capacities: Optional[List[List[int]]] = None,
    demands: Optional[List[List[int]]] = None,
    vehicle_time_windows: Optional[List[List[float]]] = None,
    task_time_windows: Optional[List[List[float]]] = None,
) -> Dict:
    """
    Build a cuOpt server request payload for a single-vehicle-type VRP/PDP problem.

    `vehicle_start_indices` and `task_indices` are indices into the shared `cost_matrix`;
    every vehicle starts and ends at its own depot location. `capacities`/`demands` and
    `vehicle_time_windows`/`task_time_windows` follow cuOpt's dimension-major shape, e.g.
    a single capacity dimension is `[[cap_v0, cap_v1, ...]]` / `[[demand_t0, demand_t1, ...]]`.
    """
    if len(vehicle_start_indices) == 0:
        raise ValueError("At least one vehicle is required.")
    if len(task_indices) == 0:
        raise ValueError("At least one task location is required.")

    request = {
        "cost_matrix_data": {"data": {"0": [list(row) for row in cost_matrix]}},
        "task_data": {"task_locations": list(task_indices)},
        "fleet_data": {"vehicle_locations": [[loc, loc] for loc in vehicle_start_indices]},
    }

    if capacities is not None:
        request["fleet_data"]["capacities"] = capacities
    if demands is not None:
        request["task_data"]["demand"] = demands
    if vehicle_time_windows is not None:
        request["fleet_data"]["vehicle_time_windows"] = vehicle_time_windows
    if task_time_windows is not None:
        request["task_data"]["task_time_windows"] = task_time_windows

    return request


def parse_vrp_response(response: Dict) -> List[VRPStep]:
    """
    Parse a cuOpt server response into a flat list of per-vehicle route steps.

    Raises CuOptSolverException if the response has no solved solution yet (e.g. it
    still needs a repoll) or if the solver did not report a success status.
    """
    solver_response = (response or {}).get("response", {}).get("solver_response")
    if solver_response is None:
        raise CuOptSolverException(f"cuOpt response did not contain a solved solution: {response}")

    status = solver_response.get("status")
    if status != CUOPT_SUCCESS_STATUS:
        error_message = solver_response.get("solver_infeasible_message") or solver_response
        raise CuOptSolverException(f"cuOpt solver did not find a feasible solution (status={status}): {error_message}")

    steps: List[VRPStep] = []
    for vehicle_id, data in solver_response.get("vehicle_data", {}).items():
        route = data.get("route", [])
        for step, (from_index, to_index) in enumerate(zip(route, route[1:])):
            steps.append(VRPStep(vehicle_id=int(vehicle_id), step=step, from_index=from_index, to_index=to_index))

    return steps


class CuOptClient:
    """
    Thin wrapper around cuopt_sh_client.CuOptServiceSelfHostClient: submits a VRP
    request to a self-hosted cuopt-server instance and repolls until a solved
    response (or timeout) is returned. This is the only piece of mage.cuopt that
    touches the network.
    """

    def __init__(self, host: str, port: int):
        if not HAS_CUOPT_SH_CLIENT:
            raise CuOptConnectionError(
                "mage.cuopt requires the 'cuopt-sh-client' package. Install it with "
                "'pip install --extra-index-url https://pypi.nvidia.com cuopt-sh-client' "
                "and point it at a running cuopt-server instance."
            )
        try:
            self._client = CuOptServiceSelfHostClient(ip=host, port=port)
        except Exception as e:
            raise CuOptConnectionError(f"Could not reach cuopt-server at {host}:{port}: {e}") from None

    def solve_vrp(self, request: Dict, repoll_tries: int = 50, repoll_interval_seconds: float = 1.0) -> Dict:
        try:
            solution = self._client.get_optimized_routes(request)
        except Exception as e:
            raise CuOptConnectionError(f"cuopt-server request failed: {e}") from None

        for _ in range(repoll_tries):
            if "reqId" not in solution or "response" in solution:
                break
            time.sleep(repoll_interval_seconds)
            try:
                solution = self._client.repoll(solution["reqId"], response_type="dict")
            except Exception as e:
                raise CuOptConnectionError(f"cuopt-server repoll failed: {e}") from None

        return solution
