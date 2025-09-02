# Common unit test function for Memgraph project
# Supports both gtest_discover_tests and regular add_test modes

set(test_unit_prefix memgraph__unit__)
set(test_bench_prefix memgraph__benchmark__)

# Only create targets if they don't already exist
if(NOT TARGET memgraph__unit)
    add_custom_target(memgraph__unit)
endif()

if(NOT TARGET memgraph__benchmark)
    add_custom_target(memgraph__benchmark)
endif()

include(GoogleTest)

function(add_unit_test exec_name)
    set(options CUSTOM_MAIN DISCOVER_TESTS)
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
    )

    # Link test framework libraries
    if(NOT ARG_CUSTOM_MAIN)
        # Try to use memgraph_unit_main if it exists, otherwise use gtest_main
        if(TARGET memgraph_unit_main)
            target_link_libraries(${target_name} PRIVATE memgraph_unit_main)
        else()
            target_link_libraries(${target_name} PRIVATE gtest_main gtest gmock Threads::Threads dl)
        endif()
    else()
        target_link_libraries(${target_name} PRIVATE gtest gmock Threads::Threads dl)
    endif()

    # Add include directories if specified
    if(ARG_INCLUDE_DIRS)
        target_include_directories(${target_name} PRIVATE ${ARG_INCLUDE_DIRS})
    endif()

    set_target_properties(${target_name} PROPERTIES OUTPUT_NAME ${exec_name})

    set(test_properties "")
    if(ARG_TEST_PROPERTIES)
        set(test_properties ${ARG_TEST_PROPERTIES})
    endif()

    # Use gtest_discover_tests if DISCOVER_TESTS is set, otherwise use add_test
    if(ARG_DISCOVER_TESTS)
        # Prepare test properties for gtest_discover_tests
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
    else()
        # Use regular add_test - runs all tests in the binary as one test
        if(TEST_COVERAGE)
            list(APPEND test_properties ENVIRONMENT "LLVM_PROFILE_FILE=${exec_name}.profraw")
        endif()

        add_test(NAME ${target_name} COMMAND $<TARGET_FILE:${target_name}>)
        if(test_properties)
            set_tests_properties(${target_name} PROPERTIES ${test_properties})
        endif()
    endif()

    add_dependencies(memgraph__unit ${target_name})
endfunction()
