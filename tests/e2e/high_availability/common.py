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
import typing

import mgclient


def get_data_path(file: str, test: str):
    """
    Data is stored in high_availability folder.
    """
    return f"high_availability/{file}/{test}"


def get_logs_path(file: str, test: str):
    """
    Logs are stored in high_availability folder.
    """
    return f"high_availability/{file}/{test}"


# Elapse time is the last element in results hence such slicing works.
def ignore_elapsed_time_from_results(results: typing.List[tuple]) -> typing.List[tuple]:
    return [result[:-1] for result in results]


def execute_and_fetch_all(cursor: mgclient.Cursor, query: str, params: dict = {}) -> typing.List[tuple]:
    cursor.execute(query, params)
    return cursor.fetchall()


def connect(**kwargs) -> mgclient.Connection:
    connection = mgclient.connect(**kwargs)
    connection.autocommit = True
    return connection


def safe_execute(function, *args):
    try:
        function(*args)
    except Exception:
        pass


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
                if instance[-1] == instance_role and instance[-2] == "up":
                    all_instances.append(instance[0])  # coordinator name

        return all_instances

    all_instances = find_instances()
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

    for inst in all_instances:
        assert inst == instance, "Leaders are not the same"

    assert instance is not None and instance != "" and len(all_instances) > 0, f"{instance_role} not found"
    return instance


def update_tuple_value(
    list_tuples: typing.List, searching_key: str, searching_index: int, index_in_tuple_value: int, new_value: str
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
