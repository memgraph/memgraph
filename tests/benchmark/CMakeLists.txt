set(test_prefix ${test_bench_prefix})

find_package(gflags REQUIRED)
find_package(antlr4-runtime REQUIRED)
find_package(range-v3 REQUIRED)

include_directories(${antlr4-runtime_INCLUDE_DIR})



function(add_benchmark test_cpp)
  # get exec name (remove extension from the abs path)
  get_filename_component(exec_name ${test_cpp} NAME_WE)
  set(target_name ${test_prefix}${exec_name})
  add_executable(${target_name} ${test_cpp} ${ARGN})
  # OUTPUT_NAME sets the real name of a target when it is built and can be
  # used to help create two targets of the same name even though CMake
  # requires unique logical target names
  set_target_properties(${target_name} PROPERTIES OUTPUT_NAME ${exec_name})
  target_link_libraries(${target_name} benchmark::benchmark gflags mg-memory)
  # register test - use modern add_test with COMMAND to get absolute path
  add_test(NAME ${target_name} COMMAND $<TARGET_FILE:${target_name}>)
  set_tests_properties(${target_name} PROPERTIES LABELS "benchmark")
  add_dependencies(memgraph__benchmark ${target_name})
endfunction(add_benchmark)

add_benchmark(data_structures/ring_buffer.cpp)
target_link_libraries(${test_prefix}ring_buffer mg-utils)

add_benchmark(query/eval.cpp)
target_link_libraries(${test_prefix}eval mg-query)

add_benchmark(query/execution.cpp ${CMAKE_SOURCE_DIR}/src/glue/communication.cpp)
target_link_libraries(${test_prefix}execution mg-query mg-communication)

add_benchmark(query/planner.cpp)
target_link_libraries(${test_prefix}planner mg-query)

add_benchmark(query/profile.cpp)
target_link_libraries(${test_prefix}profile mg-query)

add_benchmark(query/stripped.cpp)
target_link_libraries(${test_prefix}stripped mg-query)

if (MG_ENTERPRISE)
add_benchmark(rpc.cpp)
target_link_libraries(${test_prefix}rpc mg-rpc)
endif()

add_benchmark(skip_list_random.cpp)
target_link_libraries(${test_prefix}skip_list_random mg-utils)

add_benchmark(skip_list_real_world.cpp)
target_link_libraries(${test_prefix}skip_list_real_world mg-utils)

add_benchmark(skip_list_same_item.cpp)
target_link_libraries(${test_prefix}skip_list_same_item mg-utils)

add_benchmark(skip_list_vs_stl.cpp)
target_link_libraries(${test_prefix}skip_list_vs_stl mg-utils)

add_benchmark(expansion.cpp ${CMAKE_SOURCE_DIR}/src/glue/communication.cpp)
target_link_libraries(${test_prefix}expansion mg-query mg-communication mg-license)

add_benchmark(storage_v2_gc.cpp)
target_link_libraries(${test_prefix}storage_v2_gc mg::storage)

add_benchmark(storage_v2_gc2.cpp)
target_link_libraries(${test_prefix}storage_v2_gc2 mg::storage)

add_benchmark(storage_v2_property_store.cpp)
target_link_libraries(${test_prefix}storage_v2_property_store mg::storage)

add_benchmark(storage_v2_enum_store_bench.cpp)
target_link_libraries(${test_prefix}storage_v2_enum_store_bench mg::storage)

add_benchmark(thread_safe_memory.cpp)
target_link_libraries(${test_prefix}thread_safe_memory mg-utils)

add_benchmark(stack_erase_if.cpp)
target_link_libraries(${test_prefix}stack_erase_if mg-utils)

add_benchmark(snapshot_progress_bench.cpp)
target_link_libraries(${test_prefix}snapshot_progress_bench mg::storage)

# USearch index_dense benchmark: same source, two configs (builtin vs simsimd) to compare metric path.
# Uncomment to build and run locally.
#[[
set(usearch_bench_src usearch_index_add.cpp)
add_executable(${test_prefix}usearch_index_add_builtin ${usearch_bench_src})
set_target_properties(${test_prefix}usearch_index_add_builtin PROPERTIES OUTPUT_NAME usearch_index_add_builtin)
target_link_libraries(${test_prefix}usearch_index_add_builtin benchmark::benchmark gflags mg-memory usearch)
add_test(NAME ${test_prefix}usearch_index_add_builtin COMMAND $<TARGET_FILE:${test_prefix}usearch_index_add_builtin>)
set_tests_properties(${test_prefix}usearch_index_add_builtin PROPERTIES LABELS "benchmark")
add_dependencies(memgraph__benchmark ${test_prefix}usearch_index_add_builtin)

add_executable(${test_prefix}usearch_index_add_simsimd ${usearch_bench_src})
set_target_properties(${test_prefix}usearch_index_add_simsimd PROPERTIES OUTPUT_NAME usearch_index_add_simsimd)
target_link_libraries(${test_prefix}usearch_index_add_simsimd benchmark::benchmark gflags mg-memory usearch_simsimd)
add_test(NAME ${test_prefix}usearch_index_add_simsimd COMMAND $<TARGET_FILE:${test_prefix}usearch_index_add_simsimd>)
set_tests_properties(${test_prefix}usearch_index_add_simsimd PROPERTIES LABELS "benchmark")
add_dependencies(memgraph__benchmark ${test_prefix}usearch_index_add_simsimd)
]]
