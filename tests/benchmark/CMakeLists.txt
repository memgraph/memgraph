set(test_prefix memgraph__benchmark__)

add_custom_target(memgraph__benchmark)

function(add_benchmark test_cpp)
  # get exec name (remove extension from the abs path)
  get_filename_component(exec_name ${test_cpp} NAME_WE)
  set(target_name ${test_prefix}${exec_name})
  add_executable(${target_name} ${test_cpp})
  # OUTPUT_NAME sets the real name of a target when it is built and can be
  # used to help create two targets of the same name even though CMake
  # requires unique logical target names
  set_target_properties(${target_name} PROPERTIES OUTPUT_NAME ${exec_name})
  target_link_libraries(${target_name} benchmark)
  target_include_directories(${target_name} PRIVATE ${BENCHMARK_INCLUDE_DIR})
  # register test
  set(output_path
      ${CMAKE_BINARY_DIR}/test_results/benchmark/${target_name}.json)
  add_test(${target_name} ${exec_name}
           --benchmark_out_format=json --benchmark_out=${output_path})
  add_dependencies(memgraph__benchmark ${target_name})
endfunction(add_benchmark)

add_benchmark(data_structures/concurrent/map_mix_concurrent.cpp)
target_link_libraries(${test_prefix}map_mix_concurrent mg-single-node kvstore_dummy_lib)

add_benchmark(data_structures/concurrent/skiplist_insert.cpp)
target_link_libraries(${test_prefix}skiplist_insert mg-single-node kvstore_dummy_lib)

add_benchmark(data_structures/concurrent/skiplist_reverse_iteration.cpp)
target_link_libraries(${test_prefix}skiplist_reverse_iteration mg-single-node kvstore_dummy_lib)

add_benchmark(data_structures/concurrent/map_concurrent.cpp)
target_link_libraries(${test_prefix}map_concurrent mg-single-node kvstore_dummy_lib)

add_benchmark(data_structures/ring_buffer.cpp)
target_link_libraries(${test_prefix}ring_buffer mg-single-node kvstore_dummy_lib)

add_benchmark(query/planner.cpp)
target_link_libraries(${test_prefix}planner mg-single-node kvstore_dummy_lib)

add_benchmark(query/profile.cpp)
target_link_libraries(${test_prefix}profile mg-single-node kvstore_dummy_lib)

add_benchmark(query/stripped.cpp)
target_link_libraries(${test_prefix}stripped mg-single-node kvstore_dummy_lib)

add_benchmark(edge_storage.cpp)
target_link_libraries(${test_prefix}edge_storage mg-single-node kvstore_dummy_lib)

add_benchmark(mvcc.cpp)
target_link_libraries(${test_prefix}mvcc mg-single-node kvstore_dummy_lib)

add_benchmark(rpc.cpp)
target_link_libraries(${test_prefix}rpc mg-comm-rpc)

add_benchmark(serialization.cpp)
target_link_libraries(${test_prefix}serialization mg-distributed kvstore_dummy_lib)

add_benchmark(skip_list_random.cpp)
target_link_libraries(${test_prefix}skip_list_random mg-utils)

add_benchmark(skip_list_real_world.cpp)
target_link_libraries(${test_prefix}skip_list_real_world mg-utils)

add_benchmark(skip_list_same_item.cpp)
target_link_libraries(${test_prefix}skip_list_same_item mg-utils)

add_benchmark(skip_list_vs_stl.cpp)
target_link_libraries(${test_prefix}skip_list_vs_stl mg-utils)

add_benchmark(tx_engine.cpp)
target_link_libraries(${test_prefix}tx_engine mg-single-node kvstore_dummy_lib)

add_benchmark(expansion.cpp)
target_link_libraries(${test_prefix}expansion mg-single-node kvstore_dummy_lib)
