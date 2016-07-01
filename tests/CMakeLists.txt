cmake_minimum_required(VERSION 3.1)

project(memgraph_tests)

include_directories(${catch_source_dir}/include)

## UNIT TESTS

# find unit tests
file(GLOB_RECURSE unit_test_files ${CMAKE_HOME_DIRECTORY}/tests/unit/*.cpp)
get_file_names("${unit_test_files}" file_names)
set(unit_test_names "${file_names}")
message(STATUS "Available unit tests are: ${unit_test_names}")

# copy unit test data
file(COPY ${CMAKE_SOURCE_DIR}/tests/data
     DESTINATION ${CMAKE_BINARY_DIR}/tests)

set(chosen_test "concurrent_skiplist")

# build unit tests
foreach(test ${unit_test_names})
    set(test_name unit_${test})
    if (${chosen_test} STREQUAL ${test_name})
        add_executable(${test_name} unit/${test}.cpp)
        # TODO: separate dependencies
        target_link_libraries(${test_name} stdc++fs)
        target_link_libraries(${test_name} cypher_lib)
        target_link_libraries(${test_name} Threads::Threads)
        target_link_libraries(${test_name} ${fmt_static_lib})
        add_test(NAME ${test_name} COMMAND ${test_name})
        set_property(TARGET ${test_name} PROPERTY CXX_STANDARD 14)
    endif(${chosen_test} STREQUAL ${test_name})
endforeach()

## CONCURRENCY TESTS

# find concurrency tests
file(GLOB_RECURSE concurrency_test_files
     ${CMAKE_HOME_DIRECTORY}/tests/concurrent/*.cpp)
get_file_names("${concurrency_test_files}" file_names)
set(concurrency_test_names "${file_names}")
message(STATUS "Available concurrency tests are: ${concurrency_test_names}")

# build concurrency tests
foreach(test ${concurrency_test_names})
    set(test_name concurrent_${test})
    if (${chosen_test} STREQUAL ${test_name})
        add_executable(${test_name} concurrent/${test}.cpp)
        target_link_libraries(${test_name} Threads::Threads)
        add_test(NAME ${test_name} COMMAND ${test_name})
        set_property(TARGET ${test_name} PROPERTY CXX_STANDARD 14)
    endif(${chosen_test} STREQUAL ${test_name})
endforeach()
