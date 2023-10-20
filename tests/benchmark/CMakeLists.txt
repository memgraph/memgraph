set(test_prefix memgraph__benchmark__)

find_package(gflags REQUIRED)

add_custom_target(memgraph__benchmark)

function(add_benchmark test_cpp)
  # get exec name (remove extension from the abs path)
  get_filename_component(exec_name ${test_cpp} NAME_WE)
  set(target_name ${test_prefix}${exec_name})
  add_executable(${target_name} ${test_cpp} ${ARGN})
  # OUTPUT_NAME sets the real name of a target when it is built and can be
  # used to help create two targets of the same name even though CMake
  # requires unique logical target names
  set_target_properties(${target_name} PROPERTIES OUTPUT_NAME ${exec_name})
  target_link_libraries(${target_name} benchmark gflags mg-memory)
  # register test
  add_test(${target_name} ${exec_name})
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
target_link_libraries(${test_prefix}storage_v2_gc mg-storage-v2)

add_benchmark(storage_v2_property_store.cpp)
target_link_libraries(${test_prefix}storage_v2_property_store mg-storage-v2)
