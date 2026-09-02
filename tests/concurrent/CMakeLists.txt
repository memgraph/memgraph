set(test_prefix memgraph__concurrent__)

find_package(gflags)

add_custom_target(memgraph__concurrent)

function(add_concurrent_test test_cpp)
  cmake_parse_arguments(ARG "" "" "TEST_PROPERTIES" ${ARGN})
  # get exec name (remove extension from the abs path)
  get_filename_component(exec_name ${test_cpp} NAME_WE)
  set(target_name ${test_prefix}${exec_name})
  add_executable(${target_name} ${test_cpp})
  # OUTPUT_NAME sets the real name of a target when it is built and can be
  # used to help create two targets of the same name even though CMake
  # requires unique logical target names
  set_target_properties(${target_name} PROPERTIES OUTPUT_NAME ${exec_name})
  target_link_libraries(${target_name} GTest::gtest GTest::gmock GTest::gtest_main)

  set(test_properties ${ARG_TEST_PROPERTIES})
  # Every test here runs threads, so it has to say how many it uses for ctest to
  # schedule it honestly; PROCESSORS is passed per test below.
  list(FIND test_properties "TIMEOUT" timeout_index)
  if(timeout_index EQUAL -1)
    list(APPEND test_properties TIMEOUT 1800)
  endif()
  # As for the unit tests: SIGQUIT dumps core, so a test killed for hanging
  # leaves a stack behind instead of vanishing.
  list(FIND test_properties "TIMEOUT_SIGNAL_NAME" signal_index)
  if(signal_index EQUAL -1)
    list(APPEND test_properties TIMEOUT_SIGNAL_NAME SIGQUIT TIMEOUT_SIGNAL_GRACE_PERIOD 30)
  endif()

  # register test - use modern add_test with COMMAND to get absolute path
  add_test(NAME ${target_name} COMMAND $<TARGET_FILE:${target_name}>)
  set_tests_properties(${target_name} PROPERTIES LABELS "concurrent" ${test_properties})
  add_dependencies(memgraph__concurrent ${target_name})
endfunction(add_concurrent_test)

add_concurrent_test(network_read_hang.cpp TEST_PROPERTIES PROCESSORS 16)
target_link_libraries(${test_prefix}network_read_hang mg-communication)

add_concurrent_test(network_server.cpp TEST_PROPERTIES PROCESSORS 8)
target_link_libraries(${test_prefix}network_server mg-communication)

add_concurrent_test(network_session_leak.cpp TEST_PROPERTIES PROCESSORS 50)
target_link_libraries(${test_prefix}network_session_leak mg-communication)

add_concurrent_test(stack.cpp TEST_PROPERTIES PROCESSORS 4)
target_link_libraries(${test_prefix}stack mg-utils gflags)

add_concurrent_test(skip_list_insert.cpp TEST_PROPERTIES PROCESSORS 8)
target_link_libraries(${test_prefix}skip_list_insert mg-utils mg-memory)

add_concurrent_test(skip_list_insert_competitive.cpp TEST_PROPERTIES PROCESSORS 8)
target_link_libraries(${test_prefix}skip_list_insert_competitive mg-utils mg-memory)

add_concurrent_test(skip_list_estimate.cpp TEST_PROPERTIES PROCESSORS 10)
target_link_libraries(${test_prefix}skip_list_estimate mg-utils mg-memory)

add_concurrent_test(skip_list_mixed.cpp TEST_PROPERTIES PROCESSORS 10)
target_link_libraries(${test_prefix}skip_list_mixed mg-utils mg-memory)

add_concurrent_test(skip_list_remove.cpp TEST_PROPERTIES PROCESSORS 8)
target_link_libraries(${test_prefix}skip_list_remove mg-utils mg-memory)

add_concurrent_test(skip_list_remove_competitive.cpp TEST_PROPERTIES PROCESSORS 8)
target_link_libraries(${test_prefix}skip_list_remove_competitive mg-utils mg-memory)

add_concurrent_test(skip_list_find.cpp TEST_PROPERTIES PROCESSORS 2)
target_link_libraries(${test_prefix}skip_list_find mg-utils mg-memory)

add_concurrent_test(spin_lock.cpp TEST_PROPERTIES PROCESSORS 16)
target_link_libraries(${test_prefix}spin_lock mg-utils)

add_concurrent_test(storage_indices.cpp TEST_PROPERTIES PROCESSORS 6 RESOURCE_LOCK storage_memory)
target_link_libraries(${test_prefix}storage_indices mg-utils mg::storage)

add_concurrent_test(storage_unique_constraints.cpp TEST_PROPERTIES PROCESSORS 8 RESOURCE_LOCK storage_memory)
target_link_libraries(${test_prefix}storage_unique_constraints mg-utils mg::storage)

add_concurrent_test(storage_gc_soak.cpp TEST_PROPERTIES PROCESSORS 9 RESOURCE_LOCK storage_memory)
target_link_libraries(${test_prefix}storage_gc_soak mg-utils mg::storage)
