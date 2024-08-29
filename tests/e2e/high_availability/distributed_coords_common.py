# Copyright 2022 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import time
from typing import List

from common import connect, execute_and_fetch_all, ignore_elapsed_time_from_results


def get_instances_description_no_setup(temp_dir, test_name: str, use_durability: bool = True):
    return {
        "instance_1": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--management-port",
                "10011",
            ],
            f"log_file": f"high_availability/distributed_coords/{test_name}/instance_1.log",
            "data_directory": f"{temp_dir}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--management-port",
                "10012",
            ],
            "log_file": f"high_availability/distributed_coords/{test_name}/instance_2.log",
            "data_directory": f"{temp_dir}/instance_2",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7689",
                "--log-level",
                "TRACE",
                "--management-port",
                "10013",
            ],
            "log_file": f"high_availability/distributed_coords/{test_name}/instance_3.log",
            "data_directory": f"{temp_dir}/instance_3",
            "setup_queries": [],
        },
        "coordinator_1": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
                "--management-port=10121",
                f"--ha_durability={use_durability}",
                "--coordinator-hostname",
                "localhost",
            ],
            "log_file": f"high_availability/distributed_coords/{test_name}/coordinator1.log",
            "data_directory": f"{temp_dir}/coordinator_1",
            "setup_queries": [],
        },
        "coordinator_2": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7691",
                "--log-level=TRACE",
                "--coordinator-id=2",
                "--coordinator-port=10112",
                "--management-port=10122",
                f"--ha_durability={use_durability}",
                "--coordinator-hostname",
                "localhost",
            ],
            "log_file": f"high_availability/distributed_coords/{test_name}/coordinator2.log",
            "data_directory": f"{temp_dir}/coordinator_2",
            "setup_queries": [],
        },
        "coordinator_3": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7692",
                "--log-level=TRACE",
                "--coordinator-id=3",
                "--coordinator-port=10113",
                "--management-port=10123",
                f"--ha_durability={use_durability}",
                "--coordinator-hostname",
                "localhost",
            ],
            "log_file": f"high_availability/distributed_coords/{test_name}/coordinator3.log",
            "data_directory": f"{temp_dir}/coordinator_3",
            "setup_queries": [],
        },
    }


def get_default_setup_queries():
    return [
        "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
        "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
        "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
        "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
        "REGISTER INSTANCE instance_3 WITH CONFIG {'bolt_server': 'localhost:7689', 'management_server': 'localhost:10013', 'replication_server': 'localhost:10003'};",
        "SET INSTANCE instance_3 TO MAIN",
    ]


def get_instances_description_no_setup_4_coords(temp_dir, test_name: str, use_durability: bool = True):
    use_durability_str = "true" if use_durability else "false"
    return {
        "instance_1": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--management-port",
                "10011",
            ],
            "log_file": f"high_availability/distributed_coords/{test_name}/instance_1.log",
            "data_directory": f"{temp_dir}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--management-port",
                "10012",
            ],
            "log_file": f"high_availability/distributed_coords/{test_name}/instance_2.log",
            "data_directory": f"{temp_dir}/instance_2",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7689",
                "--log-level",
                "TRACE",
                "--management-port",
                "10013",
            ],
            "log_file": f"high_availability/distributed_coords/{test_name}/instance_3.log",
            "data_directory": f"{temp_dir}/instance_3",
            "setup_queries": [],
        },
        "coordinator_1": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
                f"--ha_durability={use_durability_str}",
                "--coordinator-hostname",
                "localhost",
                "--management-port=10121",
            ],
            "log_file": f"high_availability/distributed_coords/{test_name}/coordinator1.log",
            "data_directory": f"{temp_dir}/coordinator_1",
            "setup_queries": [],
        },
        "coordinator_2": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7691",
                "--log-level=TRACE",
                "--coordinator-id=2",
                "--coordinator-port=10112",
                f"--ha_durability={use_durability_str}",
                "--coordinator-hostname",
                "localhost",
                "--management-port=10122",
            ],
            "log_file": f"high_availability/distributed_coords/{test_name}/coordinator2.log",
            "data_directory": f"{temp_dir}/coordinator_2",
            "setup_queries": [],
        },
        "coordinator_3": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7692",
                "--log-level=TRACE",
                "--coordinator-id=3",
                "--coordinator-port=10113",
                f"--ha_durability={use_durability_str}",
                "--coordinator-hostname",
                "localhost",
                "--management-port=10123",
            ],
            "log_file": f"high_availability/distributed_coords/{test_name}/coordinator3.log",
            "data_directory": f"{temp_dir}/coordinator_3",
            "setup_queries": [],
        },
        "coordinator_4": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7693",
                "--log-level=TRACE",
                "--coordinator-id=4",
                "--coordinator-port=10114",
                f"--ha_durability={use_durability_str}",
                "--coordinator-hostname",
                "localhost",
                "--management-port=10124",
            ],
            "log_file": f"high_availability/distributed_coords/{test_name}/coordinator4.log",
            "data_directory": f"{temp_dir}/coordinator_4",
            "setup_queries": [],
        },
    }


def find_instance_and_assert_instances(
    instance_role: str, num_coordinators: int = 3, coord_ids_to_skip_validation=None, wait_period=10
):
    if coord_ids_to_skip_validation is None:
        coord_ids_to_skip_validation = set()

    start_time = time.time()

    def find_instances():
        all_instances = []
        for i in range(0, num_coordinators):
            if (i + 1) in coord_ids_to_skip_validation:
                continue
            coord_cursor = connect(host="localhost", port=7690 + i).cursor()

            def show_instances():
                return ignore_elapsed_time_from_results(
                    sorted(list(execute_and_fetch_all(coord_cursor, "SHOW INSTANCES;")))
                )

            instances = show_instances()
            for instance in instances:
                if instance[-1] == instance_role:
                    all_instances.append(instance[0])  # coordinator name

        return all_instances

    all_instances = []
    expected_num_instances = num_coordinators - len(coord_ids_to_skip_validation)
    while True:
        if len(all_instances) == expected_num_instances or time.time() - start_time > wait_period:
            break
        all_instances = find_instances()
        time.sleep(0.5)

    assert (
        len(all_instances) == expected_num_instances
    ), f"{instance_role}s not found, got {all_instances}, expected {expected_num_instances}, as num_coordinators: {num_coordinators}, coord_ids_to_skip_validation: {coord_ids_to_skip_validation}"

    instance = all_instances[0]

    for l in all_instances:
        assert l == instance, "Leaders are not the same"

    assert instance is not None and instance != "" and len(all_instances) > 0, f"{instance_role} not found"
    return instance


def update_tuple_value(
    list_tuples: List, searching_key: str, searching_index: int, index_in_tuple_value: int, new_value: str
):
    def find_tuple():
        for i, tuple_obj in enumerate(list_tuples):
            if tuple_obj[searching_index] != searching_key:
                continue
            return i
        return None

    index_tuple = find_tuple()
    assert index_tuple is not None, "Tuple not found"

    tuple_obj = list_tuples[index_tuple]
    tuple_obj_list = list(tuple_obj)
    tuple_obj_list[index_in_tuple_value] = new_value
    list_tuples[index_tuple] = tuple(tuple_obj_list)

    return list_tuples
