import time


def mg_sleep_and_assert(expected_value, function_to_retrieve_data, max_duration=20, time_between_attempt=0.2):
    """
    This function will keep calling the function_to_retrieve_data until the result is equal to the expected_value.
    """
    result = function_to_retrieve_data()
    start_time = time.time()
    while result != expected_value:
        duration = time.time() - start_time
        if duration > max_duration:
            assert (
                False
            ), f" mg_sleep_and_assert has tried for too long and did not get the expected result! Last result was: {result}"

        time.sleep(time_between_attempt)
        result = function_to_retrieve_data()

    return result


def mg_assert_until(expected_value, function_to_retrieve_data, max_duration=20, time_between_attempt=0.2) -> None:
    """
    Assert for max_duration that the function_to_retrieve_data returns the expected_value
    """
    start_time = time.time()
    duration = time.time() - start_time
    while duration < max_duration:
        result = function_to_retrieve_data()
        assert (result == expected_value, f"Expected result {expected_value}, got {result}")
        time.sleep(time_between_attempt)
        duration = time.time() - start_time


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
            ), f" mg_sleep_and_assert has tried for too long and did not get the expected result! Last result was: {result}"

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
            ), f" mg_sleep_and_assert has tried for too long and did not get the expected result! Last result was: {result}"

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
            ), f" mg_sleep_and_assert has tried for too long and did not get the expected result! Last result was: {result}"

        time.sleep(time_between_attempt)
        result = function_to_retrieve_data()

    return result
