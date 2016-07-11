cmake_minimum_required(VERSION 3.1)

project(memgraph_tests)

set(src_dir ${CMAKE_SOURCE_DIR}/src)

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

# build unit tests
foreach(test ${unit_test_names})
    set(test_name unit_${test})
    add_executable(${test_name} unit/${test}.cpp ${src_dir}/template_engine/engine.cpp)
    # TODO: separate dependencies
    target_link_libraries(${test_name} stdc++fs)
    target_link_libraries(${test_name} cypher_lib)
    target_link_libraries(${test_name} Threads::Threads)
    target_link_libraries(${test_name} ${fmt_static_lib})
    add_test(NAME ${test_name} COMMAND ${test_name})
    set_property(TARGET ${test_name} PROPERTY CXX_STANDARD 14)
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
    add_executable(${test_name} concurrent/${test}.cpp)
    target_link_libraries(${test_name} Threads::Threads)
    add_test(NAME ${test_name} COMMAND ${test_name})
    set_property(TARGET ${test_name} PROPERTY CXX_STANDARD 14)
endforeach()

## INTEGRATION TESTS

# build integration tests
# message(STATUS ${memgraph_src_files})
add_executable(integration_queries integration/queries.cpp ${memgraph_src_files})
target_link_libraries(integration_queries ${fmt_static_lib})
add_test(NAME integration_queries COMMAND integration_queries)
set_property(TARGET integration_queries PROPERTY CXX_STANDARD 14)

## MANUAL TESTS

add_executable(manual_query_engine manual/query_engine.cpp ${memgraph_src_files})
target_link_libraries(manual_query_engine ${fmt_static_lib})
target_link_libraries(manual_query_engine dl)
target_link_libraries(manual_query_engine cypher_lib)
set_property(TARGET manual_query_engine PROPERTY CXX_STANDARD 14)
