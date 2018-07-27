set(test_prefix memgraph__unit__)

add_custom_target(memgraph__unit)

function(add_unit_test test_cpp)
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
  set(output_path ${CMAKE_BINARY_DIR}/test_results/unit/${target_name}.xml)
  add_test(${target_name} ${exec_name} --gtest_output=xml:${output_path})
  # add to memgraph__unit target
  add_dependencies(memgraph__unit ${target_name})
endfunction(add_unit_test)

add_unit_test(bolt_encoder.cpp)
target_link_libraries(${test_prefix}bolt_encoder memgraph_lib kvstore_dummy_lib)

add_unit_test(concurrent_id_mapper_distributed.cpp)
target_link_libraries(${test_prefix}concurrent_id_mapper_distributed memgraph_lib kvstore_dummy_lib)

add_unit_test(concurrent_id_mapper_single_node.cpp)
target_link_libraries(${test_prefix}concurrent_id_mapper_single_node memgraph_lib kvstore_dummy_lib)

add_unit_test(concurrent_map_access.cpp)
target_link_libraries(${test_prefix}concurrent_map_access memgraph_lib kvstore_dummy_lib)

add_unit_test(concurrent_map.cpp)
target_link_libraries(${test_prefix}concurrent_map memgraph_lib kvstore_dummy_lib)

add_unit_test(counters.cpp)
target_link_libraries(${test_prefix}counters memgraph_lib kvstore_dummy_lib)

add_unit_test(cypher_main_visitor.cpp)
target_link_libraries(${test_prefix}cypher_main_visitor memgraph_lib kvstore_dummy_lib)

add_unit_test(database_key_index.cpp)
target_link_libraries(${test_prefix}database_key_index memgraph_lib kvstore_dummy_lib)

add_unit_test(database_label_property_index.cpp)
target_link_libraries(${test_prefix}database_label_property_index memgraph_lib kvstore_dummy_lib)

add_unit_test(database_master.cpp)
target_link_libraries(${test_prefix}database_master memgraph_lib kvstore_dummy_lib)

add_unit_test(database_transaction_timeout.cpp)
target_link_libraries(${test_prefix}database_transaction_timeout memgraph_lib kvstore_dummy_lib)

add_unit_test(datastructure_union_find.cpp)
target_link_libraries(${test_prefix}datastructure_union_find memgraph_lib kvstore_dummy_lib)

add_unit_test(deferred_deleter.cpp)
target_link_libraries(${test_prefix}deferred_deleter memgraph_lib kvstore_dummy_lib)

add_unit_test(distributed_bfs.cpp)
target_link_libraries(${test_prefix}distributed_bfs memgraph_lib kvstore_dummy_lib)

add_unit_test(distributed_coordination.cpp)
target_link_libraries(${test_prefix}distributed_coordination memgraph_lib kvstore_dummy_lib)

add_unit_test(distributed_data_exchange.cpp)
target_link_libraries(${test_prefix}distributed_data_exchange memgraph_lib kvstore_dummy_lib)

add_unit_test(distributed_durability.cpp)
target_link_libraries(${test_prefix}distributed_durability memgraph_lib kvstore_dummy_lib)

add_unit_test(distributed_dynamic_graph_partitioner.cpp)
target_link_libraries(${test_prefix}distributed_dynamic_graph_partitioner memgraph_lib kvstore_dummy_lib)

add_unit_test(distributed_gc.cpp)
target_link_libraries(${test_prefix}distributed_gc memgraph_lib kvstore_dummy_lib)

add_unit_test(distributed_graph_db.cpp)
target_link_libraries(${test_prefix}distributed_graph_db memgraph_lib kvstore_dummy_lib)

add_unit_test(distributed_interpretation.cpp)
target_link_libraries(${test_prefix}distributed_interpretation memgraph_lib kvstore_dummy_lib)

add_unit_test(distributed_query_plan.cpp)
target_link_libraries(${test_prefix}distributed_query_plan memgraph_lib kvstore_dummy_lib)

add_unit_test(distributed_reset.cpp)
target_link_libraries(${test_prefix}distributed_reset memgraph_lib kvstore_dummy_lib)

add_unit_test(distributed_serialization.cpp)
target_link_libraries(${test_prefix}distributed_serialization memgraph_lib kvstore_dummy_lib)

add_unit_test(distributed_updates.cpp)
target_link_libraries(${test_prefix}distributed_updates memgraph_lib kvstore_dummy_lib)

# add_unit_test(distributed_vertex_migrator.cpp)
# target_link_libraries(${test_prefix}distributed_vertex_migrator memgraph_lib kvstore_dummy_lib)

add_unit_test(durability.cpp)
target_link_libraries(${test_prefix}durability memgraph_lib kvstore_dummy_lib)

add_unit_test(dynamic_bitset.cpp)
target_link_libraries(${test_prefix}dynamic_bitset memgraph_lib kvstore_dummy_lib)

add_unit_test(gid.cpp)
target_link_libraries(${test_prefix}gid memgraph_lib kvstore_dummy_lib)

add_unit_test(graph_db_accessor.cpp)
target_link_libraries(${test_prefix}graph_db_accessor memgraph_lib kvstore_dummy_lib)

add_unit_test(graph_db_accessor_index_api.cpp)
target_link_libraries(${test_prefix}graph_db_accessor_index_api memgraph_lib kvstore_dummy_lib)

add_unit_test(graph_db.cpp)
target_link_libraries(${test_prefix}graph_db memgraph_lib kvstore_dummy_lib)

add_unit_test(interpreter.cpp)
target_link_libraries(${test_prefix}interpreter memgraph_lib kvstore_dummy_lib)

add_unit_test(kvstore.cpp)
target_link_libraries(${test_prefix}kvstore kvstore_lib memgraph_lib glog)

add_unit_test(metrics.cpp)
target_link_libraries(${test_prefix}metrics memgraph_lib kvstore_dummy_lib)

add_unit_test(mvcc.cpp)
target_link_libraries(${test_prefix}mvcc memgraph_lib kvstore_dummy_lib)

add_unit_test(mvcc_find.cpp)
target_link_libraries(${test_prefix}mvcc_find memgraph_lib kvstore_dummy_lib)

add_unit_test(mvcc_gc.cpp)
target_link_libraries(${test_prefix}mvcc_gc memgraph_lib kvstore_dummy_lib)

add_unit_test(mvcc_one_transaction.cpp)
target_link_libraries(${test_prefix}mvcc_one_transaction memgraph_lib kvstore_dummy_lib)

add_unit_test(mvcc_parallel_update.cpp)
target_link_libraries(${test_prefix}mvcc_parallel_update memgraph_lib kvstore_dummy_lib)

add_unit_test(pod_buffer.cpp)
target_link_libraries(${test_prefix}pod_buffer memgraph_lib kvstore_dummy_lib)

add_unit_test(property_value_store.cpp)
target_link_libraries(${test_prefix}property_value_store kvstore_lib memgraph_lib)

add_unit_test(query_cost_estimator.cpp)
target_link_libraries(${test_prefix}query_cost_estimator memgraph_lib kvstore_dummy_lib)

add_unit_test(query_expression_evaluator.cpp)
target_link_libraries(${test_prefix}query_expression_evaluator memgraph_lib kvstore_dummy_lib)

add_unit_test(query_plan_accumulate_aggregate.cpp)
target_link_libraries(${test_prefix}query_plan_accumulate_aggregate memgraph_lib kvstore_dummy_lib)

add_unit_test(query_plan_bag_semantics.cpp)
target_link_libraries(${test_prefix}query_plan_bag_semantics memgraph_lib kvstore_dummy_lib)

add_unit_test(query_plan_create_set_remove_delete.cpp)
target_link_libraries(${test_prefix}query_plan_create_set_remove_delete memgraph_lib kvstore_dummy_lib)

add_unit_test(query_plan_edge_cases.cpp)
target_link_libraries(${test_prefix}query_plan_edge_cases memgraph_lib kvstore_dummy_lib)

add_unit_test(query_plan_match_filter_return.cpp)
target_link_libraries(${test_prefix}query_plan_match_filter_return memgraph_lib kvstore_dummy_lib)

add_unit_test(query_planner.cpp)
target_link_libraries(${test_prefix}query_planner memgraph_lib kvstore_dummy_lib)

add_unit_test(query_semantic.cpp)
target_link_libraries(${test_prefix}query_semantic memgraph_lib kvstore_dummy_lib)

add_unit_test(query_variable_start_planner.cpp)
target_link_libraries(${test_prefix}query_variable_start_planner memgraph_lib kvstore_dummy_lib)

add_unit_test(queue.cpp)
target_link_libraries(${test_prefix}queue memgraph_lib kvstore_dummy_lib)

add_unit_test(raft.cpp)
target_link_libraries(${test_prefix}raft memgraph_lib kvstore_dummy_lib)

add_unit_test(raft_storage.cpp)
target_link_libraries(${test_prefix}raft_storage memgraph_lib kvstore_dummy_lib)

add_unit_test(record_edge_vertex_accessor.cpp)
target_link_libraries(${test_prefix}record_edge_vertex_accessor memgraph_lib kvstore_dummy_lib)

add_unit_test(rpc_worker_clients.cpp)
target_link_libraries(${test_prefix}rpc_worker_clients memgraph_lib kvstore_dummy_lib)

add_unit_test(serialization.cpp)
target_link_libraries(${test_prefix}serialization memgraph_lib kvstore_dummy_lib)

add_unit_test(skiplist_access.cpp)
target_link_libraries(${test_prefix}skiplist_access memgraph_lib kvstore_dummy_lib)

add_unit_test(skiplist_gc.cpp)
target_link_libraries(${test_prefix}skiplist_gc memgraph_lib kvstore_dummy_lib)

add_unit_test(skiplist_position_and_count.cpp)
target_link_libraries(${test_prefix}skiplist_position_and_count memgraph_lib kvstore_dummy_lib)

add_unit_test(skiplist_reverse_iteration.cpp)
target_link_libraries(${test_prefix}skiplist_reverse_iteration memgraph_lib kvstore_dummy_lib)

add_unit_test(skiplist_suffix.cpp)
target_link_libraries(${test_prefix}skiplist_suffix memgraph_lib kvstore_dummy_lib)

add_unit_test(state_delta.cpp)
target_link_libraries(${test_prefix}state_delta memgraph_lib kvstore_dummy_lib)

add_unit_test(static_bitset.cpp)
target_link_libraries(${test_prefix}static_bitset memgraph_lib kvstore_dummy_lib)

add_unit_test(storage_address.cpp)
target_link_libraries(${test_prefix}storage_address memgraph_lib kvstore_dummy_lib)

add_unit_test(stripped.cpp)
target_link_libraries(${test_prefix}stripped memgraph_lib kvstore_dummy_lib)

add_unit_test(transaction_engine_distributed.cpp)
target_link_libraries(${test_prefix}transaction_engine_distributed memgraph_lib kvstore_dummy_lib)

add_unit_test(transaction_engine_single_node.cpp)
target_link_libraries(${test_prefix}transaction_engine_single_node memgraph_lib kvstore_dummy_lib)

add_unit_test(typed_value.cpp)
target_link_libraries(${test_prefix}typed_value memgraph_lib kvstore_dummy_lib)

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
target_link_libraries(${test_prefix}rpc mg-communication)

# Test data structures

add_unit_test(ring_buffer.cpp)
target_link_libraries(${test_prefix}ring_buffer mg-utils)

# Test mg-io

add_unit_test(network_endpoint.cpp)
target_link_libraries(${test_prefix}network_endpoint mg-io)
add_unit_test(network_utils.cpp)
target_link_libraries(${test_prefix}network_utils mg-io)


# Test mg-utils

add_unit_test(utils_algorithm.cpp)
target_link_libraries(${test_prefix}utils_algorithm mg-utils)

add_unit_test(utils_demangle.cpp)
target_link_libraries(${test_prefix}utils_demangle mg-utils)

add_unit_test(utils_exceptions.cpp)
target_link_libraries(${test_prefix}utils_exceptions mg-utils)

add_unit_test(utils_executor.cpp)
target_link_libraries(${test_prefix}utils_executor mg-utils)

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

add_unit_test(utils_thread_pool.cpp)
target_link_libraries(${test_prefix}utils_thread_pool mg-utils)

add_unit_test(utils_timestamp.cpp)
target_link_libraries(${test_prefix}utils_timestamp mg-utils)

add_unit_test(utils_watchdog.cpp)
target_link_libraries(${test_prefix}utils_watchdog mg-utils)

# Test mg-auth

add_unit_test(auth.cpp)
target_link_libraries(${test_prefix}auth mg-auth kvstore_lib)
