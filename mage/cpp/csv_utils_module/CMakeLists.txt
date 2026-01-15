set(csv_utils_src
    csv_utils_module.cpp
    algorithm/csv_utils.cpp)

add_query_module(csv_utils 1 "${csv_utils_src}")
