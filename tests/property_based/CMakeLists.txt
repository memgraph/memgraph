set(test_prefix memgraph__property_based__)

add_custom_target(memgraph__property_based)

find_package(GTest REQUIRED)
find_package(rapidcheck REQUIRED)

function(add_property_based_test test_cpp)
    get_filename_component(exec_name ${test_cpp} NAME_WE)
    set(target_name ${test_prefix}${exec_name})
    add_executable(${target_name} ${test_cpp})
    set_target_properties(${target_name} PROPERTIES OUTPUT_NAME ${exec_name})


    target_link_libraries(${target_name}
        gtest:
	gtest_main
	rapidcheck::rapidcheck
	rapidcheck_gtest
    )

    add_test(${target_name} ${exec_name})
    add_dependencies(memgraph__property_based ${target_name})
endfunction()

add_property_based_test(random_graph.cpp)
target_link_libraries(${test_prefix}random_graph mg::storage)
