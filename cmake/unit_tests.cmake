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
include(CheckLinkerFlag)

# ~9M R_X86_64_RELATIVE relocations per test binary sit in a ~206MB .rela.dyn that ld.so reads and
# applies in full before main(), on every one of the 200-odd test starts in a suite run. DT_RELR
# encodes the same relocations as a bitmap and takes a few MB: measured on
# memgraph__unit__query_expression_evaluator, .rela.dyn falls from 206MB to 0.47MB against 2.3MB of
# .relr.dyn, and the loadable image with it, from 801MB to 601MB. Scoped to test binaries on
# purpose: DT_RELR needs glibc 2.36 on the machine that runs it, which holds for anything building
# the tests but not for every platform a released memgraph package has to start on.
check_linker_flag(CXX "LINKER:-z,pack-relative-relocs" MG_HAVE_PACK_RELATIVE_RELOCS)

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
            target_link_libraries(${target_name} PRIVATE gtest_main GTest::gtest GTest::gmock Threads::Threads dl)
        endif()
    else()
        target_link_libraries(${target_name} PRIVATE GTest::gtest GTest::gmock Threads::Threads dl)
    endif()

    # Add include directories if specified
    if(ARG_INCLUDE_DIRS)
        target_include_directories(${target_name} PRIVATE ${ARG_INCLUDE_DIRS})
    endif()

    set_target_properties(${target_name} PROPERTIES OUTPUT_NAME ${exec_name})

    if(MG_HAVE_PACK_RELATIVE_RELOCS)
        target_link_options(${target_name} PRIVATE "LINKER:-z,pack-relative-relocs")
    endif()

    set(test_properties "")
    if(ARG_TEST_PROPERTIES)
        set(test_properties ${ARG_TEST_PROPERTIES})
    endif()

    # This project calls enable_testing() without including the CTest module, so
    # ctest applies no timeout of its own: a test that hangs runs until whatever
    # is driving ctest gives up, and the report names no test. Cap it here, above
    # the slowest test so that only a hang trips it, and near enough to it that a
    # hang is reached well before the job's own limit. A test that needs longer
    # passes its own TIMEOUT in TEST_PROPERTIES, and still gets the signal
    # settings below.
    list(FIND test_properties "TIMEOUT" timeout_index)
    if(timeout_index EQUAL -1)
        list(APPEND test_properties TIMEOUT 1200)
    endif()

    # Kill a timed-out test with SIGQUIT, whose default disposition dumps core, so the
    # hang leaves a stack behind for tools/ci/core-dumps to symbolise. SIGKILL, which
    # ctest sends once the grace period is up, leaves nothing to look at. The grace
    # period is there for the core to be written; ctest would otherwise allow one
    # second, which will not do it for a process this size.
    #
    # The two are guarded separately. A caller naming only the signal still wants a
    # usable window, and one naming only the window would otherwise lose it, since
    # the later value of a repeated test property is the one that takes effect.
    list(FIND test_properties "TIMEOUT_SIGNAL_NAME" signal_index)
    if(signal_index EQUAL -1)
        list(APPEND test_properties TIMEOUT_SIGNAL_NAME SIGQUIT)
    endif()

    list(FIND test_properties "TIMEOUT_SIGNAL_GRACE_PERIOD" grace_index)
    if(grace_index EQUAL -1)
        list(APPEND test_properties TIMEOUT_SIGNAL_GRACE_PERIOD 30)
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
                PROPERTIES ${test_properties} LABELS "unit"
            )
        else()
            gtest_discover_tests(${target_name}
                TEST_PREFIX "${target_name}."
                PROPERTIES LABELS "unit"
            )
        endif()
    else()
        # Use regular add_test - runs all tests in the binary as one test
        if(TEST_COVERAGE)
            list(APPEND test_properties ENVIRONMENT "LLVM_PROFILE_FILE=${exec_name}.profraw")
        endif()

        add_test(NAME ${target_name} COMMAND $<TARGET_FILE:${target_name}>)
        if(test_properties)
            set_tests_properties(${target_name} PROPERTIES ${test_properties} LABELS "unit")
        else()
            set_tests_properties(${target_name} PROPERTIES LABELS "unit")
        endif()
    endif()

    add_dependencies(memgraph__unit ${target_name})
endfunction()
