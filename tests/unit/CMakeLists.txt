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
  target_link_libraries(${target_name} gtest gmock gtest_main Threads::Threads)
  # register test
  add_test(${target_name} ${exec_name})
  # add to memgraph__unit target
  add_dependencies(memgraph__unit ${target_name})
endfunction(add_unit_test)

add_unit_test(ast_serialization.cpp)
target_link_libraries(${test_prefix}ast_serialization mg-distributed kvstore_dummy_lib)

add_unit_test(bolt_encoder.cpp)
target_link_libraries(${test_prefix}bolt_encoder mg-single-node kvstore_dummy_lib)

add_unit_test(cache.cpp)
target_link_libraries(${test_prefix}cache mg-distributed kvstore_dummy_lib)

add_unit_test(concurrent_id_mapper_distributed.cpp)
target_link_libraries(${test_prefix}concurrent_id_mapper_distributed mg-distributed kvstore_dummy_lib)

add_unit_test(concurrent_id_mapper_single_node.cpp)
target_link_libraries(${test_prefix}concurrent_id_mapper_single_node mg-single-node kvstore_dummy_lib)

add_unit_test(concurrent_map_access.cpp)
target_link_libraries(${test_prefix}concurrent_map_access mg-single-node kvstore_dummy_lib)

add_unit_test(concurrent_map.cpp)
target_link_libraries(${test_prefix}concurrent_map mg-single-node kvstore_dummy_lib)

add_unit_test(cypher_main_visitor.cpp)
target_link_libraries(${test_prefix}cypher_main_visitor mg-single-node kvstore_dummy_lib)

add_unit_test(database_dump.cpp)
target_link_libraries(${test_prefix}database_dump mg-single-node kvstore_dummy_lib)

add_unit_test(database_key_index.cpp)
target_link_libraries(${test_prefix}database_key_index mg-single-node kvstore_dummy_lib)

add_unit_test(database_label_property_index.cpp)
target_link_libraries(${test_prefix}database_label_property_index mg-single-node kvstore_dummy_lib)

add_unit_test(database_master.cpp)
target_link_libraries(${test_prefix}database_master mg-distributed kvstore_dummy_lib)

add_unit_test(database_transaction_timeout.cpp)
target_link_libraries(${test_prefix}database_transaction_timeout mg-single-node kvstore_dummy_lib)

add_unit_test(datastructure_union_find.cpp)
target_link_libraries(${test_prefix}datastructure_union_find mg-single-node kvstore_dummy_lib)

add_unit_test(deferred_deleter.cpp)
target_link_libraries(${test_prefix}deferred_deleter mg-single-node kvstore_dummy_lib)

add_unit_test(distributed_coordination.cpp)
target_link_libraries(${test_prefix}distributed_coordination mg-distributed kvstore_dummy_lib)

add_unit_test(distributed_data_exchange.cpp)
target_link_libraries(${test_prefix}distributed_data_exchange mg-distributed kvstore_dummy_lib)

add_unit_test(distributed_dgp_vertex_migrator.cpp)
target_link_libraries(${test_prefix}distributed_dgp_vertex_migrator mg-distributed kvstore_dummy_lib)

add_unit_test(distributed_durability.cpp)
target_link_libraries(${test_prefix}distributed_durability mg-distributed kvstore_dummy_lib)

add_unit_test(distributed_dynamic_worker.cpp)
target_link_libraries(${test_prefix}distributed_dynamic_worker mg-distributed kvstore_dummy_lib)

add_unit_test(distributed_edges_iterator.cpp)
target_link_libraries(${test_prefix}distributed_edges_iterator mg-distributed kvstore_dummy_lib)

add_unit_test(distributed_gc.cpp)
target_link_libraries(${test_prefix}distributed_gc mg-distributed kvstore_dummy_lib)

add_unit_test(distributed_graph_db.cpp)
target_link_libraries(${test_prefix}distributed_graph_db mg-distributed kvstore_dummy_lib)

add_unit_test(distributed_interpretation.cpp)
target_link_libraries(${test_prefix}distributed_interpretation mg-distributed kvstore_dummy_lib)

add_unit_test(distributed_plan_pretty_print.cpp)
target_link_libraries(${test_prefix}distributed_plan_pretty_print mg-distributed kvstore_dummy_lib)

add_unit_test(distributed_query_plan.cpp)
target_link_libraries(${test_prefix}distributed_query_plan mg-distributed kvstore_dummy_lib)

add_unit_test(distributed_reset.cpp)
target_link_libraries(${test_prefix}distributed_reset mg-distributed kvstore_dummy_lib)

add_unit_test(distributed_serialization.cpp)
target_link_libraries(${test_prefix}distributed_serialization mg-distributed kvstore_dummy_lib)

add_unit_test(distributed_updates.cpp)
target_link_libraries(${test_prefix}distributed_updates mg-distributed kvstore_dummy_lib)

# TODO (buda): Replace token sharing with centralized solution and write an appropriate test.
# add_unit_test(distributed_token_sharing.cpp)
# target_link_libraries(${test_prefix}distributed_token_sharing memgraph_lib kvstore_dummy_lib)

add_unit_test(bfs_distributed.cpp)
target_link_libraries(${test_prefix}bfs_distributed mg-distributed kvstore_dummy_lib)

add_unit_test(bfs_single_node.cpp)
target_link_libraries(${test_prefix}bfs_single_node mg-single-node kvstore_dummy_lib)

add_unit_test(distributed_dgp_partitioner.cpp)
target_link_libraries(${test_prefix}distributed_dgp_partitioner mg-distributed kvstore_dummy_lib)

add_unit_test(durability.cpp)
target_link_libraries(${test_prefix}durability mg-single-node kvstore_dummy_lib)

add_unit_test(dynamic_bitset.cpp)
target_link_libraries(${test_prefix}dynamic_bitset mg-single-node kvstore_dummy_lib)

add_unit_test(edges_distributed.cpp)
target_link_libraries(${test_prefix}edges_distributed mg-distributed kvstore_dummy_lib)

add_unit_test(edges_single_node.cpp)
target_link_libraries(${test_prefix}edges_single_node mg-single-node kvstore_dummy_lib)

add_unit_test(gid.cpp)
target_link_libraries(${test_prefix}gid mg-distributed kvstore_dummy_lib)

add_unit_test(graph_db_accessor.cpp)
target_link_libraries(${test_prefix}graph_db_accessor mg-single-node kvstore_dummy_lib)

add_unit_test(graph_db_accessor_index_api.cpp)
target_link_libraries(${test_prefix}graph_db_accessor_index_api mg-single-node kvstore_dummy_lib)

add_unit_test(graph_db.cpp)
target_link_libraries(${test_prefix}graph_db mg-single-node kvstore_dummy_lib)

add_unit_test(interpreter.cpp)
target_link_libraries(${test_prefix}interpreter mg-single-node kvstore_dummy_lib)

add_unit_test(kvstore.cpp)
target_link_libraries(${test_prefix}kvstore kvstore_lib glog)

add_unit_test(metrics.cpp)
target_link_libraries(${test_prefix}metrics mg-stats)

add_unit_test(mvcc.cpp)
target_link_libraries(${test_prefix}mvcc mg-single-node kvstore_dummy_lib)

add_unit_test(mvcc_find.cpp)
target_link_libraries(${test_prefix}mvcc_find mg-single-node kvstore_dummy_lib)

add_unit_test(mvcc_gc.cpp)
target_link_libraries(${test_prefix}mvcc_gc mg-single-node kvstore_dummy_lib)

add_unit_test(mvcc_one_transaction.cpp)
target_link_libraries(${test_prefix}mvcc_one_transaction mg-single-node kvstore_dummy_lib)

add_unit_test(mvcc_parallel_update.cpp)
target_link_libraries(${test_prefix}mvcc_parallel_update mg-single-node kvstore_dummy_lib)

add_unit_test(pod_buffer.cpp)
target_link_libraries(${test_prefix}pod_buffer mg-single-node kvstore_dummy_lib)

add_unit_test(property_value_store.cpp)
target_link_libraries(${test_prefix}property_value_store kvstore_lib mg-single-node)

add_unit_test(replication_log.cpp)
target_link_libraries(${test_prefix}replication_log mg-single-node-ha kvstore_lib glog)

add_unit_test(query_cost_estimator.cpp)
target_link_libraries(${test_prefix}query_cost_estimator mg-single-node kvstore_dummy_lib)

add_unit_test(query_expression_evaluator.cpp)
target_link_libraries(${test_prefix}query_expression_evaluator mg-single-node kvstore_dummy_lib)

add_unit_test(plan_pretty_print.cpp)
target_link_libraries(${test_prefix}plan_pretty_print mg-single-node kvstore_dummy_lib)

add_unit_test(query_pretty_print.cpp)
target_link_libraries(${test_prefix}query_pretty_print mg-single-node kvstore_dummy_lib)

add_unit_test(query_profile.cpp)
target_link_libraries(${test_prefix}query_profile mg-single-node kvstore_dummy_lib)

add_unit_test(query_plan_accumulate_aggregate.cpp)
target_link_libraries(${test_prefix}query_plan_accumulate_aggregate mg-single-node kvstore_dummy_lib)

add_unit_test(query_plan_bag_semantics.cpp)
target_link_libraries(${test_prefix}query_plan_bag_semantics mg-single-node kvstore_dummy_lib)

add_unit_test(query_plan_create_set_remove_delete.cpp)
target_link_libraries(${test_prefix}query_plan_create_set_remove_delete mg-single-node kvstore_dummy_lib)

add_unit_test(query_plan_edge_cases.cpp)
target_link_libraries(${test_prefix}query_plan_edge_cases mg-single-node kvstore_dummy_lib)

add_unit_test(query_plan_match_filter_return.cpp)
target_link_libraries(${test_prefix}query_plan_match_filter_return mg-single-node kvstore_dummy_lib)

add_unit_test(query_plan.cpp)
target_link_libraries(${test_prefix}query_plan mg-single-node kvstore_dummy_lib)

add_unit_test(query_required_privileges.cpp)
target_link_libraries(${test_prefix}query_required_privileges mg-single-node kvstore_dummy_lib)

add_unit_test(query_semantic.cpp)
target_link_libraries(${test_prefix}query_semantic mg-single-node kvstore_dummy_lib)

add_unit_test(query_variable_start_planner.cpp)
target_link_libraries(${test_prefix}query_variable_start_planner mg-single-node kvstore_dummy_lib)

add_unit_test(queue.cpp)
target_link_libraries(${test_prefix}queue mg-single-node kvstore_dummy_lib)

add_unit_test(record_edge_vertex_accessor.cpp)
target_link_libraries(${test_prefix}record_edge_vertex_accessor mg-single-node kvstore_dummy_lib)

add_unit_test(skip_list.cpp)
target_link_libraries(${test_prefix}skip_list mg-utils)

add_unit_test(skiplist_access.cpp)
target_link_libraries(${test_prefix}skiplist_access mg-single-node kvstore_dummy_lib)

add_unit_test(skiplist_gc.cpp)
target_link_libraries(${test_prefix}skiplist_gc mg-single-node kvstore_dummy_lib)

add_unit_test(skiplist_position_and_count.cpp)
target_link_libraries(${test_prefix}skiplist_position_and_count mg-single-node kvstore_dummy_lib)

add_unit_test(skiplist_reverse_iteration.cpp)
target_link_libraries(${test_prefix}skiplist_reverse_iteration mg-single-node kvstore_dummy_lib)

add_unit_test(skiplist_suffix.cpp)
target_link_libraries(${test_prefix}skiplist_suffix mg-single-node kvstore_dummy_lib)

add_unit_test(slk_advanced.cpp)
target_link_libraries(${test_prefix}slk_advanced mg-distributed kvstore_dummy_lib)

add_unit_test(slk_core.cpp)
target_link_libraries(${test_prefix}slk_core mg-slk glog gflags fmt)

add_unit_test(slk_streams.cpp)
target_link_libraries(${test_prefix}slk_streams mg-slk glog gflags fmt)

add_unit_test(small_vector.cpp)
target_link_libraries(${test_prefix}small_vector mg-utils)

add_unit_test(state_delta.cpp)
target_link_libraries(${test_prefix}state_delta mg-single-node kvstore_dummy_lib)

add_unit_test(static_bitset.cpp)
target_link_libraries(${test_prefix}static_bitset mg-single-node kvstore_dummy_lib)

add_unit_test(storage_address.cpp)
target_link_libraries(${test_prefix}storage_address mg-distributed kvstore_dummy_lib)

add_unit_test(storage_stat.cpp)
target_link_libraries(${test_prefix}storage_stat mg-single-node kvstore_dummy_lib)

add_unit_test(stripped.cpp)
target_link_libraries(${test_prefix}stripped mg-single-node kvstore_dummy_lib)

add_unit_test(transaction_engine_distributed.cpp)
target_link_libraries(${test_prefix}transaction_engine_distributed mg-distributed kvstore_dummy_lib)

add_unit_test(transaction_engine_single_node.cpp)
target_link_libraries(${test_prefix}transaction_engine_single_node mg-single-node kvstore_dummy_lib)

add_unit_test(transaction_engine_single_node_ha.cpp)
target_link_libraries(${test_prefix}transaction_engine_single_node_ha mg-single-node-ha kvstore_dummy_lib)

add_unit_test(typed_value.cpp)
target_link_libraries(${test_prefix}typed_value mg-single-node kvstore_dummy_lib)

add_unit_test(unique_constraints.cpp)
target_link_libraries(${test_prefix}unique_constraints mg-single-node kvstore_dummy_lib)

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

add_unit_test(utils_demangle.cpp)
target_link_libraries(${test_prefix}utils_demangle mg-utils)

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

add_unit_test(utils_thread_pool.cpp)
target_link_libraries(${test_prefix}utils_thread_pool mg-utils)

add_unit_test(utils_timestamp.cpp)
target_link_libraries(${test_prefix}utils_timestamp mg-utils)

add_unit_test(utils_watchdog.cpp)
target_link_libraries(${test_prefix}utils_watchdog mg-utils)

# Test mg-auth

add_unit_test(auth.cpp)
target_link_libraries(${test_prefix}auth mg-auth kvstore_lib)

# Test storage v2

add_unit_test(storage_v2.cpp)
target_link_libraries(${test_prefix}storage_v2 mg-utils)

# Test LCP

add_custom_command(
  OUTPUT test_lcp
  DEPENDS ${lcp_src_files} lcp test_lcp.lisp
  COMMAND sbcl --script ${CMAKE_CURRENT_SOURCE_DIR}/test_lcp.lisp)
add_custom_target(test_lcp ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/test_lcp)
add_test(test_lcp ${CMAKE_CURRENT_BINARY_DIR}/test_lcp)
add_dependencies(memgraph__unit test_lcp)
