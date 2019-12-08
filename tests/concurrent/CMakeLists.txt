set(test_prefix memgraph__concurrent__)

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
  target_link_libraries(${target_name} gtest gmock gtest_main)
  # register test
  add_test(${target_name} ${exec_name})
  add_dependencies(memgraph__concurrent ${target_name})
endfunction(add_concurrent_test)

add_concurrent_test(dynamic_bitset_clear_n.cpp)
target_link_libraries(${test_prefix}dynamic_bitset_clear_n mg-single-node kvstore_dummy_lib)

add_concurrent_test(dynamic_bitset.cpp)
target_link_libraries(${test_prefix}dynamic_bitset mg-single-node kvstore_dummy_lib)

add_concurrent_test(dynamic_bitset_set.cpp)
target_link_libraries(${test_prefix}dynamic_bitset_set mg-single-node kvstore_dummy_lib)

add_concurrent_test(dynamic_bitset_set_n.cpp)
target_link_libraries(${test_prefix}dynamic_bitset_set_n mg-single-node kvstore_dummy_lib)

add_concurrent_test(network_read_hang.cpp)
target_link_libraries(${test_prefix}network_read_hang mg-communication)

add_concurrent_test(network_server.cpp)
target_link_libraries(${test_prefix}network_server mg-communication)

add_concurrent_test(network_session_leak.cpp)
target_link_libraries(${test_prefix}network_session_leak mg-communication)

add_concurrent_test(push_queue.cpp)
target_link_libraries(${test_prefix}push_queue glog gflags Threads::Threads)

add_concurrent_test(stack.cpp)
target_link_libraries(${test_prefix}stack mg-utils)

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

add_concurrent_test(sl_hang.cpp)
target_link_libraries(${test_prefix}sl_hang mg-single-node kvstore_dummy_lib)

add_concurrent_test(sl_insert_competetive.cpp)
target_link_libraries(${test_prefix}sl_insert_competetive mg-single-node kvstore_dummy_lib)

add_concurrent_test(sl_insert.cpp)
target_link_libraries(${test_prefix}sl_insert mg-single-node kvstore_dummy_lib)

add_concurrent_test(sl_map.cpp)
target_link_libraries(${test_prefix}sl_map mg-single-node kvstore_dummy_lib)

add_concurrent_test(sl_memory.cpp)
target_link_libraries(${test_prefix}sl_memory mg-single-node kvstore_dummy_lib)

add_concurrent_test(sl_memory_leak.cpp)
target_link_libraries(${test_prefix}sl_memory_leak mg-single-node kvstore_dummy_lib)

add_concurrent_test(sl_remove_competetive.cpp)
target_link_libraries(${test_prefix}sl_remove_competetive mg-single-node kvstore_dummy_lib)

add_concurrent_test(sl_remove_disjoint.cpp)
target_link_libraries(${test_prefix}sl_remove_disjoint mg-single-node kvstore_dummy_lib)

add_concurrent_test(sl_remove_joint.cpp)
target_link_libraries(${test_prefix}sl_remove_joint mg-single-node kvstore_dummy_lib)

add_concurrent_test(sl_set.cpp)
target_link_libraries(${test_prefix}sl_set mg-single-node kvstore_dummy_lib)

add_concurrent_test(sl_simulation.cpp)
target_link_libraries(${test_prefix}sl_simulation mg-single-node kvstore_dummy_lib)

add_concurrent_test(spin_lock.cpp)
target_link_libraries(${test_prefix}spin_lock mg-utils)
