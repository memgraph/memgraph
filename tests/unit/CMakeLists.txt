set(test_prefix memgraph__unit__)

add_custom_target(memgraph__unit)

function(add_unit_test test_cpp)
  # get exec name (remove extension from the abs path)
  get_filename_component(exec_name ${test_cpp} NAME_WE)
  set(target_name ${test_prefix}${exec_name})
  add_executable(${target_name} ${test_cpp} ${ARGN})
  # OUTPUT_NAME sets the real name of a target when it is built and can be
  # used to help create two targets of the same name even though CMake
  # requires unique logical target names
  set_target_properties(${target_name} PROPERTIES OUTPUT_NAME ${exec_name})
  target_link_libraries(${target_name} gtest gmock gtest_main Threads::Threads)
  # register test
  add_test(${target_name} ${exec_name})
  # add to memgraph__unit target
  add_dependencies(memgraph__unit ${target_name})
endfunction(add_unit_test)

add_unit_test(bolt_encoder.cpp ${CMAKE_SOURCE_DIR}/src/glue/communication.cpp)
target_link_libraries(${test_prefix}bolt_encoder mg-communication mg-query)

add_unit_test(commit_log_v2.cpp)
target_link_libraries(${test_prefix}commit_log_v2 glog gflags)

add_unit_test(kvstore.cpp)
target_link_libraries(${test_prefix}kvstore kvstore_lib glog)

#add_unit_test(replication_log.cpp)
#target_link_libraries(${test_prefix}replication_log mg-single-node-ha kvstore_lib glog)

# Test mg-query

add_unit_test(cypher_main_visitor.cpp)
target_link_libraries(${test_prefix}cypher_main_visitor mg-query)

add_unit_test(bfs_single_node.cpp)
target_link_libraries(${test_prefix}bfs_single_node mg-query)

add_unit_test(interpreter.cpp ${CMAKE_SOURCE_DIR}/src/glue/communication.cpp)
target_link_libraries(${test_prefix}interpreter mg-communication mg-query)

add_unit_test(query_cost_estimator.cpp)
target_link_libraries(${test_prefix}query_cost_estimator mg-query)

add_unit_test(query_expression_evaluator.cpp)
target_link_libraries(${test_prefix}query_expression_evaluator mg-query)

add_unit_test(plan_pretty_print.cpp)
target_link_libraries(${test_prefix}plan_pretty_print mg-query)

add_unit_test(query_dump.cpp ${CMAKE_SOURCE_DIR}/src/glue/communication.cpp)
target_link_libraries(${test_prefix}query_dump mg-communication mg-query)

add_unit_test(query_pretty_print.cpp)
target_link_libraries(${test_prefix}query_pretty_print mg-query)

add_unit_test(query_profile.cpp)
target_link_libraries(${test_prefix}query_profile mg-query)

add_unit_test(query_plan_accumulate_aggregate.cpp)
target_link_libraries(${test_prefix}query_plan_accumulate_aggregate mg-query)

add_unit_test(query_plan_bag_semantics.cpp)
target_link_libraries(${test_prefix}query_plan_bag_semantics mg-query)

add_unit_test(query_plan_create_set_remove_delete.cpp)
target_link_libraries(${test_prefix}query_plan_create_set_remove_delete mg-query)

# Storage V2 in query execution
add_unit_test(query_plan_v2_create_set_remove_delete.cpp)
target_link_libraries(${test_prefix}query_plan_v2_create_set_remove_delete mg-query)
# END Storage V2 in query execution

add_unit_test(query_plan_edge_cases.cpp ${CMAKE_SOURCE_DIR}/src/glue/communication.cpp)
target_link_libraries(${test_prefix}query_plan_edge_cases mg-communication mg-query)

add_unit_test(query_plan_match_filter_return.cpp)
target_link_libraries(${test_prefix}query_plan_match_filter_return mg-query)

add_unit_test(query_plan.cpp)
target_link_libraries(${test_prefix}query_plan mg-query)

# Test query/procedure
add_unit_test(query_procedure_mgp_type.cpp)
target_link_libraries(${test_prefix}query_procedure_mgp_type mg-query)
target_include_directories(${test_prefix}query_procedure_mgp_type PRIVATE ${CMAKE_SOURCE_DIR}/include)

add_unit_test(query_procedure_mgp_module.cpp)
target_link_libraries(${test_prefix}query_procedure_mgp_module mg-query)
target_include_directories(${test_prefix}query_procedure_mgp_module PRIVATE ${CMAKE_SOURCE_DIR}/include)
# END query/procedure

add_unit_test(query_required_privileges.cpp)
target_link_libraries(${test_prefix}query_required_privileges mg-query)

add_unit_test(query_semantic.cpp)
target_link_libraries(${test_prefix}query_semantic mg-query)

add_unit_test(query_variable_start_planner.cpp)
target_link_libraries(${test_prefix}query_variable_start_planner mg-query)

add_unit_test(stripped.cpp)
target_link_libraries(${test_prefix}stripped mg-query)

add_unit_test(typed_value.cpp)
target_link_libraries(${test_prefix}typed_value mg-query)

# END mg-query

add_unit_test(skip_list.cpp)
target_link_libraries(${test_prefix}skip_list mg-utils)

## TODO: REPLACE single-node-ha
#add_unit_test(slk_advanced.cpp)
#target_link_libraries(${test_prefix}slk_advanced mg-single-node-ha kvstore_dummy_lib)

add_unit_test(slk_core.cpp)
target_link_libraries(${test_prefix}slk_core mg-slk glog gflags fmt)

add_unit_test(slk_streams.cpp)
target_link_libraries(${test_prefix}slk_streams mg-slk glog gflags fmt)

add_unit_test(small_vector.cpp)
target_link_libraries(${test_prefix}small_vector mg-utils)

# Test mg-communication

add_unit_test(bolt_chunked_decoder_buffer.cpp)
target_link_libraries(${test_prefix}bolt_chunked_decoder_buffer mg-communication)

add_unit_test(bolt_chunked_encoder_buffer.cpp)
target_link_libraries(${test_prefix}bolt_chunked_encoder_buffer mg-communication)

add_unit_test(bolt_decoder.cpp)
target_link_libraries(${test_prefix}bolt_decoder mg-communication)

add_unit_test(bolt_session.cpp)
target_link_libraries(${test_prefix}bolt_session mg-communication)

add_unit_test(communication_buffer.cpp)
target_link_libraries(${test_prefix}communication_buffer mg-communication)

add_unit_test(network_timeouts.cpp)
target_link_libraries(${test_prefix}network_timeouts mg-communication)

add_unit_test(rpc.cpp)
target_link_libraries(${test_prefix}rpc mg-comm-rpc)

# Test data structures

add_unit_test(ring_buffer.cpp)
target_link_libraries(${test_prefix}ring_buffer mg-utils)

# Test mg-io

add_unit_test(network_endpoint.cpp)
target_link_libraries(${test_prefix}network_endpoint mg-io)

add_unit_test(network_utils.cpp)
target_link_libraries(${test_prefix}network_utils mg-io)

add_unit_test(socket.cpp)
target_link_libraries(${test_prefix}socket mg-io)

# Test mg-utils

add_unit_test(utils_algorithm.cpp)
target_link_libraries(${test_prefix}utils_algorithm mg-utils)

add_unit_test(utils_exceptions.cpp)
target_link_libraries(${test_prefix}utils_exceptions mg-utils)

add_unit_test(utils_executor.cpp)
target_link_libraries(${test_prefix}utils_executor mg-utils)

add_unit_test(utils_file.cpp)
target_link_libraries(${test_prefix}utils_file mg-utils)

add_unit_test(utils_math.cpp)
target_link_libraries(${test_prefix}utils_math mg-utils)

add_unit_test(utils_memory.cpp)
target_link_libraries(${test_prefix}utils_memory mg-utils)

add_unit_test(utils_on_scope_exit.cpp)
target_link_libraries(${test_prefix}utils_on_scope_exit mg-utils)

add_unit_test(utils_scheduler.cpp)
target_link_libraries(${test_prefix}utils_scheduler mg-utils)

add_unit_test(utils_rwlock.cpp)
target_link_libraries(${test_prefix}utils_rwlock mg-utils)

add_unit_test(utils_signals.cpp)
target_link_libraries(${test_prefix}utils_signals mg-utils)

add_unit_test(utils_string.cpp)
target_link_libraries(${test_prefix}utils_string mg-utils)

add_unit_test(utils_synchronized.cpp)
target_link_libraries(${test_prefix}utils_synchronized mg-utils)

add_unit_test(utils_timestamp.cpp)
target_link_libraries(${test_prefix}utils_timestamp mg-utils)

# Test mg-auth

add_unit_test(auth.cpp)
target_link_libraries(${test_prefix}auth mg-auth)

# Test storage v2

add_unit_test(property_value_v2.cpp)
target_link_libraries(${test_prefix}property_value_v2 mg-utils)

add_unit_test(storage_v2_constraints.cpp)
target_link_libraries(${test_prefix}storage_v2_constraints mg-storage-v2)

add_unit_test(storage_v2_decoder_encoder.cpp)
target_link_libraries(${test_prefix}storage_v2_decoder_encoder mg-storage-v2)

add_unit_test(storage_v2_durability.cpp)
target_link_libraries(${test_prefix}storage_v2_durability mg-storage-v2)

add_unit_test(storage_v2_edge.cpp)
target_link_libraries(${test_prefix}storage_v2_edge mg-storage-v2)

add_unit_test(storage_v2.cpp)
target_link_libraries(${test_prefix}storage_v2 mg-storage-v2)

add_unit_test(storage_v2_gc.cpp)
target_link_libraries(${test_prefix}storage_v2_gc mg-storage-v2)

add_unit_test(storage_v2_indices.cpp)
target_link_libraries(${test_prefix}storage_v2_indices mg-storage-v2)

add_unit_test(storage_v2_name_id_mapper.cpp)
target_link_libraries(${test_prefix}storage_v2_name_id_mapper mg-storage-v2)

add_unit_test(storage_v2_property_store.cpp)
target_link_libraries(${test_prefix}storage_v2_property_store mg-storage-v2 fmt)

add_unit_test(storage_v2_wal_file.cpp)
target_link_libraries(${test_prefix}storage_v2_wal_file mg-storage-v2 fmt)

# Test LCP

add_custom_command(
  OUTPUT test_lcp
  DEPENDS ${lcp_src_files} lcp test_lcp.lisp
  COMMAND sbcl --script ${CMAKE_CURRENT_SOURCE_DIR}/test_lcp.lisp)
add_custom_target(test_lcp ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/test_lcp)
add_test(test_lcp ${CMAKE_CURRENT_BINARY_DIR}/test_lcp)
add_dependencies(memgraph__unit test_lcp)
