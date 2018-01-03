# set current directory name as a test type
get_filename_component(test_type ${CMAKE_CURRENT_SOURCE_DIR} NAME)

# get all cpp abs file names recursively starting from current directory
file(GLOB_RECURSE test_type_cpps *.cpp)
message(STATUS "Available ${test_type} cpp files are: ${test_type_cpps}")

# for each cpp file build binary and register test
foreach(test_cpp ${test_type_cpps})

    # get exec name (remove extension from the abs path)
    get_filename_component(exec_name ${test_cpp} NAME_WE)

    set(target_name memgraph__${test_type}__${exec_name})

    # build exec file
    add_executable(${target_name} ${test_cpp})
    if(TEST_COVERAGE)
        set_target_properties(${target_name} PROPERTIES COMPILE_FLAGS "-g -O0 -Wall -fprofile-arcs -ftest-coverage")
        set_target_properties(${target_name} PROPERTIES LINK_FLAGS "--coverage -fprofile-arcs -ftest-coverage")
    endif()

    # OUTPUT_NAME sets the real name of a target when it is built and can be
    # used to help create two targets of the same name even though CMake
    # requires unique logical target names
    set_target_properties(${target_name} PROPERTIES OUTPUT_NAME ${exec_name})

    # link libraries
    target_link_libraries(${target_name} memgraph_lib)
    if(TEST_COVERAGE)
        # for code coverage
        target_link_libraries(${target_name} gcov)
    endif()

    set(output_path ${CMAKE_BINARY_DIR}/test_results/unit/${target_name}.xml)

endforeach()
