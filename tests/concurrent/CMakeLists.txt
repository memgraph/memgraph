set(test_prefix memgraph__concurrent__)

find_package(gflags)

add_custom_target(memgraph__concurrent)

function(add_concurrent_test test_cpp)
  # get exec name (remove extension from the abs path)
  get_filename_component(exec_name ${test_cpp} NAME_WE)
  set(target_name ${test_prefix}${exec_name})
  add_executable(${target_name} ${test_cpp})
  # OUTPUT_NAME sets the real name of a target when it is built and can be
  # used to help create two targets of the same name even though CMake
  # requires unique logical target names
  set_target_properties(${target_name} PROPERTIES OUTPUT_NAME ${exec_name})
  target_link_libraries(${target_name} GTest::gtest GTest::gmock GTest::gtest_main)
  # register test
  add_test(${target_name} ${exec_name})
  add_dependencies(memgraph__concurrent ${target_name})
endfunction(add_concurrent_test)

add_concurrent_test(network_read_hang.cpp)
target_link_libraries(${test_prefix}network_read_hang mg-communication)

add_concurrent_test(network_server.cpp)
target_link_libraries(${test_prefix}network_server mg-communication)

add_concurrent_test(network_session_leak.cpp)
target_link_libraries(${test_prefix}network_session_leak mg-communication)

add_concurrent_test(stack.cpp)
target_link_libraries(${test_prefix}stack mg-utils gflags)

add_concurrent_test(skip_list_insert.cpp)
target_link_libraries(${test_prefix}skip_list_insert mg-utils)

add_concurrent_test(skip_list_insert_competitive.cpp)
target_link_libraries(${test_prefix}skip_list_insert_competitive mg-utils)

add_concurrent_test(skip_list_mixed.cpp)
target_link_libraries(${test_prefix}skip_list_mixed mg-utils)

add_concurrent_test(skip_list_remove.cpp)
target_link_libraries(${test_prefix}skip_list_remove mg-utils)

add_concurrent_test(skip_list_remove_competitive.cpp)
target_link_libraries(${test_prefix}skip_list_remove_competitive mg-utils)

add_concurrent_test(skip_list_find.cpp)
target_link_libraries(${test_prefix}skip_list_find mg-utils)

add_concurrent_test(spin_lock.cpp)
target_link_libraries(${test_prefix}spin_lock mg-utils)

add_concurrent_test(storage_indices.cpp)
target_link_libraries(${test_prefix}storage_indices mg-utils mg::storage)

add_concurrent_test(storage_unique_constraints.cpp)
target_link_libraries(${test_prefix}storage_unique_constraints mg-utils mg::storage)
