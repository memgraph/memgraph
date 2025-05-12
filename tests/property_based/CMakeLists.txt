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
    target_link_libraries(${target_name} gtest gtest_main)
    target_link_libraries(${target_name} rapidcheck rapidcheck_gtest)
    add_test(${target_name} ${exec_name})
    add_dependencies(memgraph__property_based ${target_name})
endfunction(add_property_based_test)

add_property_based_test(random_graph.cpp)
target_link_libraries(${test_prefix}random_graph mg::storage)
