import time
from typing import Set


def mg_sleep_and_assert_eval_function(
    eval_function, function_to_retrieve_data, max_duration=20, time_between_attempt=0.2
):
    """
    Check if eval_function executed on the result of function_to_retrieve_data return true;
    """
    result = function_to_retrieve_data()
    start_time = time.time()
    while eval_function(result) is not True:
        duration = time.time() - start_time
        if duration > max_duration:
            assert (
                False
            ), f" mg_sleep_and_assert_eval_function has tried for too long and did not get the expected result! Last result was: {result}."

        time.sleep(time_between_attempt)
        result = function_to_retrieve_data()

    return result


def mg_sleep_and_assert(expected_value, function_to_retrieve_data, max_duration=20, time_between_attempt=0.2):
    """"""
    result = function_to_retrieve_data()
    start_time = time.time()
    while result != expected_value:
        duration = time.time() - start_time
        if duration > max_duration:
            assert (
                False
            ), f" mg_sleep_and_assert has tried for too long and did not get the expected result! Expected result {expected_value}, Last result was: {result}"

        time.sleep(time_between_attempt)
        result = function_to_retrieve_data()

    return result


def wait_for_status_change(
    function_to_retrieve_data,
    instance_names: Set[str],
    new_state: str,
    max_duration: int = 20,
    time_between_attempt: int = 0.5,
):
    result = function_to_retrieve_data()

    start_time = time.time()

    while True:
        for instance_state in result:
            if instance_state[0] in instance_names:
                if instance_state[-1] == new_state:
                    return

        duration = time.time() - start_time
        if duration > max_duration:
            assert (
                False
            ), f"wait_for_status_change has tried for too long and did not get the expected result! Last result was: {result}"

        time.sleep(time_between_attempt)
        result = function_to_retrieve_data()


def mg_sleep_and_assert_multiple(
    expected_values, functions_to_retrieve_data, max_duration=20, time_between_attempt=0.2
):
    """
    This function will keep calling the functions in functions_to_retrieve_data until one of them returns a value that is in expected_values.
    """
    result = [f() for f in functions_to_retrieve_data]
    if any((x in expected_values for x in result)):
        return True
    start_time = time.time()
    while True:
        duration = time.time() - start_time
        if duration > max_duration:
            assert (
                False
            ), f" mg_sleep_and_assert has tried for too long and did not get the expected result! Expected result was {expected_values}. Last result was: {result}"

        time.sleep(time_between_attempt)
        result = [f() for f in functions_to_retrieve_data]
        if any((x in expected_values for x in result)):
            return True


def mg_sleep_and_assert_any_function(
    expected_value, functions_to_retrieve_data, max_duration=20, time_between_attempt=0.2
):
    """
    This function will keep calling the functions in functions_to_retrieve_data until one of them returns the expected value.
    """
    result = [f() for f in functions_to_retrieve_data]
    if any((x == expected_value for x in result)):
        return result
    start_time = time.time()
    while result != expected_value:
        duration = time.time() - start_time
        if duration > max_duration:
            assert (
                False
            ), f" mg_sleep_and_assert has tried for too long and did not get the expected result! Expected result was {expected_value}. Last result was: {result}"

        time.sleep(time_between_attempt)
        result = [f() for f in functions_to_retrieve_data]
        if any((x == expected_value for x in result)):
            return result

    return result


def mg_sleep_and_assert_collection(
    expected_value, function_to_retrieve_data, max_duration=20, time_between_attempt=0.2
):
    """
    This function will keep calling the function_to_retrieve_data until the result is equal to the expected_value.
    """
    result = function_to_retrieve_data()
    start_time = time.time()
    while len(result) != len(expected_value) or any((x not in result for x in expected_value)):
        duration = time.time() - start_time
        if duration > max_duration:
            assert (
                False
            ), f" mg_sleep_and_assert has tried for too long and did not get the expected result! Expected result was {expected_value}. Last result was: {result}"

        time.sleep(time_between_attempt)
        result = function_to_retrieve_data()

    return result


def mg_sleep_and_assert_until_role_change(
    function_to_execute, expected_role, max_duration=20, time_between_attempts=0.2
):
    result = function_to_execute()
    start_time = time.time()
    while result != expected_role:
        duration = time.time() - start_time
        if duration > max_duration:
            assert (
                False
            ), " mg_sleep_and_assert_until_role_change has tried for too long and did not get the expected result!"

        time.sleep(time_between_attempts)
        result = function_to_execute()
