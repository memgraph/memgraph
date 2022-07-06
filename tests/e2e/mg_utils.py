import time


def mg_sleep_and_assert(expected_value, function_to_retrieve_data, max_duration=20, time_between_attempt=0.5):
    result = function_to_retrieve_data()
    start_time = time.time()
    while result != expected_value:
        current_time = time.time()
        duration = current_time - start_time
        if duration > max_duration:
            assert False, " mg_sleep_and_assert has tried for too long and did not get the expected result!"

        time.sleep(time_between_attempt)
        result = function_to_retrieve_data()

    return result
