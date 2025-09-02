include(GoogleTest)
function(add_unit_test exec_name)
    set(options CUSTOM_MAIN)
    set(oneValueArgs "")
    set(multiValueArgs SOURCES LINK_TARGETS TEST_PROPERTIES INCLUDE_DIRS)
    cmake_parse_arguments(ARG "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

    set(target_name ${test_unit_prefix}${exec_name})

    add_executable(${target_name})
    target_sources(${target_name}
        PRIVATE
        ${ARG_SOURCES}
    )

    target_link_libraries(${target_name}
        PRIVATE
        ${ARG_LINK_TARGETS}
        gtest gmock Threads::Threads dl
    )

    # Link test framework libraries
    if(NOT ARG_CUSTOM_MAIN)
        # Try to use memgraph_unit_main if it exists, otherwise use gtest_main
        if(TARGET memgraph_unit_main)
            target_link_libraries(${target_name} PRIVATE memgraph_unit_main)
        else()
            target_link_libraries(${target_name} PRIVATE gtest_main)
        endif()
    endif()

    # Add include directories if specified
    if(ARG_INCLUDE_DIRS)
        target_include_directories(${target_name} PRIVATE ${ARG_INCLUDE_DIRS})
    endif()

    set_target_properties(${target_name} PROPERTIES OUTPUT_NAME ${exec_name})

    # Prepare test properties for gtest_discover_tests
    set(test_properties "")
    if(ARG_TEST_PROPERTIES)
        set(test_properties ${ARG_TEST_PROPERTIES})
    endif()
    if(TEST_COVERAGE)
        list(APPEND test_properties ENVIRONMENT "LLVM_PROFILE_FILE=${exec_name}_%p_%m.profraw")
    endif()

    if(test_properties)
        gtest_discover_tests(${target_name}
            TEST_PREFIX "${target_name}."
            PROPERTIES ${test_properties}
        )
    else()
        gtest_discover_tests(${target_name}
            TEST_PREFIX "${target_name}."
        )
    endif()

    add_dependencies(memgraph__unit ${target_name})
endfunction()

# Add planner unit tests
add_unit_test(planner
    LINK_TARGETS mg::planner
    SOURCES union_find_test.cpp
)
