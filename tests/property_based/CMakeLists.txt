set(test_prefix memgraph__property_based__)

add_custom_target(memgraph__property_based)

function(add_property_based_test test_cpp)
    # get exec name (remove extension from the abs path)
    get_filename_component(exec_name ${test_cpp} NAME_WE)
    set(target_name ${test_prefix}${exec_name})
    add_executable(${target_name} ${test_cpp})
    # OUTPUT_NAME sets the real name of a target when it is built and can be
    # used to help create two targets of the same name even though CMake
    # requires unique logical target names
    set_target_properties(${target_name} PROPERTIES OUTPUT_NAME ${exec_name})
    target_link_libraries(${target_name} GTest::gtest GTest::gtest_main)
    target_link_libraries(${target_name} rapidcheck rapidcheck_gtest)
    # register test - use modern add_test with COMMAND to get absolute path
    add_test(NAME ${target_name} COMMAND $<TARGET_FILE:${target_name}>)
    set_tests_properties(${target_name} PROPERTIES LABELS "property_based")
    add_dependencies(memgraph__property_based ${target_name})
endfunction(add_property_based_test)

add_property_based_test(random_graph.cpp)
target_link_libraries(${test_prefix}random_graph mg::storage)

# Draws values in the terms the enumerated shapes are written in
add_library(value_generators value_generators.cpp)
target_link_libraries(value_generators mg::storage value_shapes rapidcheck)
target_include_directories(value_generators PUBLIC ${CMAKE_SOURCE_DIR})

add_property_based_test(value_generator_coverage.cpp)
target_link_libraries(${test_prefix}value_generator_coverage mg::storage value_shapes value_generators)

add_library(typed_value_generators typed_value_generators.cpp)
target_link_libraries(typed_value_generators mg-query typed_value_shapes rapidcheck)
target_include_directories(typed_value_generators PUBLIC ${CMAKE_SOURCE_DIR})

add_property_based_test(typed_value_generator_coverage.cpp)
target_link_libraries(${test_prefix}typed_value_generator_coverage mg-query typed_value_shapes typed_value_generators)

add_property_based_test(relation_laws.cpp)
target_link_libraries(${test_prefix}relation_laws mg-query typed_value_generators)

add_property_based_test(storage_relation_laws.cpp)
target_link_libraries(${test_prefix}storage_relation_laws mg::storage value_generators)
