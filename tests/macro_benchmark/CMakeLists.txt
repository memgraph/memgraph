set(test_prefix memgraph__macro_benchmark__)

add_custom_target(memgraph__macro_benchmark)

function(add_macro_benchmark test_cpp)
  # get exec name (remove extension from the abs path)
  get_filename_component(exec_name ${test_cpp} NAME_WE)
  set(target_name ${test_prefix}${exec_name})
  add_executable(${target_name} ${test_cpp})
  # OUTPUT_NAME sets the real name of a target when it is built and can be
  # used to help create two targets of the same name even though CMake
  # requires unique logical target names
  set_target_properties(${target_name} PROPERTIES OUTPUT_NAME ${exec_name})
  add_dependencies(memgraph__macro_benchmark ${target_name})
endfunction(add_macro_benchmark)

add_macro_benchmark(clients/pokec_client.cpp)
target_link_libraries(${test_prefix}pokec_client mg-communication mg-io mg-utils nlohmann_json::nlohmann_json)

add_macro_benchmark(clients/graph_500_bfs.cpp)
target_link_libraries(${test_prefix}graph_500_bfs mg-communication mg-io mg-utils nlohmann_json::nlohmann_json)

add_macro_benchmark(clients/bfs_pokec_client.cpp)
target_link_libraries(${test_prefix}bfs_pokec_client mg-communication mg-io mg-utils nlohmann_json::nlohmann_json)

add_macro_benchmark(clients/query_client.cpp)
target_link_libraries(${test_prefix}query_client mg-communication mg-io mg-utils)

add_macro_benchmark(clients/card_fraud_client.cpp)
target_link_libraries(${test_prefix}card_fraud_client mg-communication mg-io mg-utils nlohmann_json::nlohmann_json)
