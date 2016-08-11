cmake_minimum_required(VERSION 3.1)

project(memgraph_tests)

set(src_dir ${CMAKE_SOURCE_DIR}/src)

include_directories(${catch_source_dir}/include)

# TODO: modular approach (REFACTOR)
# add_executable()
# target_link_libraries()
# add_test(NAME COMMAND)
# set_property(TARGET PROPERTY CXX_STANDARD 14)

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
    target_link_libraries(${test_name} memgraph)
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
    target_link_libraries(${test_name} memgraph)
    target_link_libraries(${test_name} Threads::Threads)
    target_link_libraries(${test_name} ${fmt_static_lib})
    add_test(NAME ${test_name} COMMAND ${test_name})
    set_property(TARGET ${test_name} PROPERTY CXX_STANDARD 14)
endforeach()

## INTEGRATION TESTS

# test hard coded queries
add_executable(integration_queries integration/queries.cpp)
target_link_libraries(integration_queries memgraph)
target_link_libraries(integration_queries Threads::Threads)
target_link_libraries(integration_queries ${fmt_static_lib})
add_test(NAME integration_queries COMMAND integration_queries)
set_property(TARGET integration_queries PROPERTY CXX_STANDARD 14)

# test query engine
add_executable(integration_query_engine integration/query_engine.cpp)
target_link_libraries(integration_query_engine Threads::Threads)
add_test(NAME integration_query_engine COMMAND integration_query_engine)
set_property(TARGET integration_query_engine PROPERTY CXX_STANDARD 14)

# test memgraph with bolt protocol
add_executable(integration_memgraph_bolt integration/memgraph_bolt.cpp)
target_link_libraries(integration_memgraph_bolt Threads::Threads)
add_test(NAME integration_memgraph_bolt COMMAND integration_memgraph_bolt)
set_property(TARGET integration_memgraph_bolt PROPERTY CXX_STANDARD 14)

## MANUAL TESTS

# cypher_ast
add_executable(manual_cypher_ast manual/cypher_ast.cpp)
target_link_libraries(manual_cypher_ast memgraph)
target_link_libraries(manual_cypher_ast Threads::Threads)
target_link_libraries(manual_cypher_ast ${fmt_static_lib})
target_link_libraries(manual_cypher_ast cypher_lib)
set_property(TARGET manual_cypher_ast PROPERTY CXX_STANDARD 14)

# queries
add_executable(manual_queries manual/queries.cpp)
target_link_libraries(manual_queries memgraph)
target_link_libraries(manual_queries Threads::Threads)
target_link_libraries(manual_queries ${fmt_static_lib})
target_link_libraries(manual_queries cypher_lib)
set_property(TARGET manual_queries PROPERTY CXX_STANDARD 14)

# query_engine
add_executable(manual_query_engine manual/query_engine.cpp)
target_link_libraries(manual_query_engine memgraph)
target_link_libraries(manual_query_engine ${fmt_static_lib})
target_link_libraries(manual_query_engine dl)
target_link_libraries(manual_query_engine cypher_lib)
target_link_libraries(manual_query_engine Threads::Threads)
set_property(TARGET manual_query_engine PROPERTY CXX_STANDARD 14)

# query_hasher
add_executable(manual_query_hasher manual/query_hasher.cpp)
target_link_libraries(manual_query_hasher memgraph)
target_link_libraries(manual_query_hasher ${fmt_static_lib})
set_property(TARGET manual_query_hasher PROPERTY CXX_STANDARD 14)

# query_action_processor
add_executable(manual_cpp_generator manual/cpp_generator.cpp)
target_link_libraries(manual_cpp_generator memgraph)
target_link_libraries(manual_cpp_generator ${fmt_static_lib})
set_property(TARGET manual_cpp_generator PROPERTY CXX_STANDARD 14)
