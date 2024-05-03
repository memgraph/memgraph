set(test_prefix memgraph__unit__)

find_package(fmt REQUIRED)
find_package(gflags REQUIRED)
find_package(Threads REQUIRED)

add_custom_target(memgraph__unit)

add_library(memgraph_unit_main OBJECT main.cpp)
target_link_libraries(memgraph_unit_main mg-memory mg-utils gtest gmock Threads::Threads dl)

function(add_unit_test test_cpp)
  _add_unit_test(${test_cpp} FALSE ${ARGN})
endfunction(add_unit_test)

function(add_unit_test_with_custom_main test_cpp)
  _add_unit_test(${test_cpp} TRUE ${ARGN})
endfunction(add_unit_test_with_custom_main)

function(_add_unit_test test_cpp custom_main)
  # get exec name (remove extension from the abs path)
  get_filename_component(exec_name ${test_cpp} NAME_WE)
  set(target_name ${test_prefix}${exec_name})

  set(source_files
    ${test_cpp}
    ${ARGN})

  add_executable(${target_name} ${source_files})

  if(NOT ${custom_main})
    target_link_libraries(${target_name} memgraph_unit_main)
  else()
    target_link_libraries(${target_name} gtest gmock Threads::Threads dl)
  endif()

  # OUTPUT_NAME sets the real name of a target when it is built and can be
  # used to help create two targets of the same name even though CMake
  # requires unique logical target names
  set_target_properties(${target_name} PROPERTIES OUTPUT_NAME ${exec_name})


  # register test
  if(TEST_COVERAGE)
    add_test(${target_name} env LLVM_PROFILE_FILE=${exec_name}.profraw ./${exec_name})
  else()
    add_test(${target_name} ${exec_name})
  endif()

  # add to memgraph__unit target
  add_dependencies(memgraph__unit ${target_name})
endfunction(_add_unit_test)

# Test utilities
add_library(storage_test_utils storage_test_utils.cpp)
target_link_libraries(storage_test_utils mg-storage-v2)

# Test integrations-kafka
add_library(kafka-mock STATIC kafka_mock.cpp)
target_link_libraries(kafka-mock mg-utils librdkafka++ librdkafka Threads::Threads gtest)

# Include directories are intentionally not set, because kafka-mock isn't meant to be used apart from unit tests
add_unit_test(integrations_kafka_consumer.cpp kafka_mock.cpp)
target_link_libraries(${test_prefix}integrations_kafka_consumer kafka-mock mg-integrations-kafka)

add_unit_test(mgp_kafka_c_api.cpp)
target_link_libraries(${test_prefix}mgp_kafka_c_api mg-query mg-integrations-kafka)

add_unit_test(mgp_trans_c_api.cpp)
target_link_libraries(${test_prefix}mgp_trans_c_api mg-query)

# Test mg-query
add_unit_test(bfs_single_node.cpp)
target_link_libraries(${test_prefix}bfs_single_node mg-query mg-glue)

add_unit_test(bfs_fine_grained.cpp)
target_link_libraries(${test_prefix}bfs_fine_grained mg-query mg-glue)

add_unit_test(cypher_main_visitor.cpp)
target_link_libraries(${test_prefix}cypher_main_visitor mg-query)

add_unit_test(interpreter.cpp ${CMAKE_SOURCE_DIR}/src/glue/communication.cpp)
target_link_libraries(${test_prefix}interpreter mg-communication mg-query mg-glue)

add_unit_test(plan_pretty_print.cpp)
target_link_libraries(${test_prefix}plan_pretty_print mg-query)

add_unit_test(query_cost_estimator.cpp)
target_link_libraries(${test_prefix}query_cost_estimator mg-query)

add_unit_test(query_dump.cpp ${CMAKE_SOURCE_DIR}/src/glue/communication.cpp)
target_link_libraries(${test_prefix}query_dump mg-communication mg-query)

add_unit_test(query_expression_evaluator.cpp)
target_link_libraries(${test_prefix}query_expression_evaluator mg-query)

add_unit_test(query_plan.cpp)
target_link_libraries(${test_prefix}query_plan mg-query)

add_unit_test(query_plan_accumulate_aggregate.cpp)
target_link_libraries(${test_prefix}query_plan_accumulate_aggregate mg-query mg-glue)

add_unit_test(query_plan_bag_semantics.cpp)
target_link_libraries(${test_prefix}query_plan_bag_semantics mg-query mg-glue)

add_unit_test(query_plan_create_set_remove_delete.cpp)
target_link_libraries(${test_prefix}query_plan_create_set_remove_delete mg-query mg-glue)

add_unit_test(query_plan_edge_cases.cpp ${CMAKE_SOURCE_DIR}/src/glue/communication.cpp)
target_link_libraries(${test_prefix}query_plan_edge_cases mg-communication mg-query)

add_unit_test(query_plan_match_filter_return.cpp)
target_link_libraries(${test_prefix}query_plan_match_filter_return mg-query mg-glue)

add_unit_test(query_plan_operator_to_string.cpp)
target_link_libraries(${test_prefix}query_plan_operator_to_string mg-query)

add_unit_test(query_plan_read_write_typecheck.cpp
  ${CMAKE_SOURCE_DIR}/src/query/plan/read_write_type_checker.cpp)
target_link_libraries(${test_prefix}query_plan_read_write_typecheck mg-query)

add_unit_test(query_plan_v2_create_set_remove_delete.cpp)
target_link_libraries(${test_prefix}query_plan_v2_create_set_remove_delete mg-query mg-glue)

add_unit_test(query_pretty_print.cpp)
target_link_libraries(${test_prefix}query_pretty_print mg-query)

add_unit_test(query_trigger.cpp)
target_link_libraries(${test_prefix}query_trigger mg-query mg-glue)

add_unit_test(query_serialization_property_value.cpp)
target_link_libraries(${test_prefix}query_serialization_property_value mg-query)

add_unit_test(query_streams.cpp)
target_link_libraries(${test_prefix}query_streams mg-query kafka-mock)

add_unit_test(transaction_queue.cpp)
target_link_libraries(${test_prefix}transaction_queue mg-communication mg-query mg-glue)

add_unit_test(transaction_queue_multiple.cpp)
target_link_libraries(${test_prefix}transaction_queue_multiple mg-communication mg-query mg-glue)

# Test query functions
add_unit_test(query_function_mgp_module.cpp)
target_link_libraries(${test_prefix}query_function_mgp_module mg-query)
target_include_directories(${test_prefix}query_function_mgp_module PRIVATE ${CMAKE_SOURCE_DIR}/include)

# Test query/procedure
add_unit_test(query_procedure_mgp_type.cpp)
target_link_libraries(${test_prefix}query_procedure_mgp_type mg-query)
target_include_directories(${test_prefix}query_procedure_mgp_type PRIVATE ${CMAKE_SOURCE_DIR}/include)

add_unit_test(query_procedure_mgp_module.cpp)
target_link_libraries(${test_prefix}query_procedure_mgp_module mg-query)
target_include_directories(${test_prefix}query_procedure_mgp_module PRIVATE ${CMAKE_SOURCE_DIR}/include)

add_unit_test_with_custom_main(query_procedure_py_module.cpp)
target_link_libraries(${test_prefix}query_procedure_py_module mg-query)
target_include_directories(${test_prefix}query_procedure_py_module PRIVATE ${CMAKE_SOURCE_DIR}/include)

add_unit_test(query_procedures_mgp_graph.cpp)
target_link_libraries(${test_prefix}query_procedures_mgp_graph mg-query storage_test_utils)
target_include_directories(${test_prefix}query_procedures_mgp_graph PRIVATE ${CMAKE_SOURCE_DIR}/include)

# END query/procedure
add_unit_test(query_profile.cpp)
target_link_libraries(${test_prefix}query_profile mg-query)

add_unit_test(query_required_privileges.cpp)
target_link_libraries(${test_prefix}query_required_privileges mg-query)

add_unit_test(query_semantic.cpp)
target_link_libraries(${test_prefix}query_semantic mg-query)

add_unit_test(query_variable_start_planner.cpp)
target_link_libraries(${test_prefix}query_variable_start_planner mg-query mg-glue)

add_unit_test(stripped.cpp)
target_link_libraries(${test_prefix}stripped mg-query)

add_unit_test(typed_value.cpp)
target_link_libraries(${test_prefix}typed_value mg-query)

# Test mg-communication
add_unit_test(bolt_chunked_decoder_buffer.cpp)
target_link_libraries(${test_prefix}bolt_chunked_decoder_buffer mg-communication)

add_unit_test(bolt_chunked_encoder_buffer.cpp)
target_link_libraries(${test_prefix}bolt_chunked_encoder_buffer mg-communication)

add_unit_test(bolt_decoder.cpp)
target_link_libraries(${test_prefix}bolt_decoder mg-communication)

add_unit_test(bolt_encoder.cpp ${CMAKE_SOURCE_DIR}/src/glue/communication.cpp)
target_link_libraries(${test_prefix}bolt_encoder mg-communication mg-query)

add_unit_test(bolt_session.cpp)
target_link_libraries(${test_prefix}bolt_session mg-communication mg-utils)

add_unit_test(communication_buffer.cpp)
target_link_libraries(${test_prefix}communication_buffer mg-communication mg-utils)

add_unit_test(network_timeouts.cpp)
target_link_libraries(${test_prefix}network_timeouts mg-communication)

# Test mg-kvstore
add_unit_test(kvstore.cpp)
target_link_libraries(${test_prefix}kvstore mg-kvstore mg-utils)

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

add_unit_test(utils_histogram.cpp)
target_link_libraries(${test_prefix}utils_histogram mg-utils mg-events)

add_unit_test(utils_file.cpp)
target_link_libraries(${test_prefix}utils_file mg-utils)

add_unit_test(utils_math.cpp)
target_link_libraries(${test_prefix}utils_math mg-utils)

add_unit_test(utils_memory.cpp)
target_link_libraries(${test_prefix}utils_memory mg-utils)

add_unit_test(utils_memory_tracker.cpp)
target_link_libraries(${test_prefix}utils_memory_tracker mg-utils)

add_unit_test(utils_on_scope_exit.cpp)
target_link_libraries(${test_prefix}utils_on_scope_exit mg-utils)

add_unit_test(utils_rwlock.cpp)
target_link_libraries(${test_prefix}utils_rwlock mg-utils)

add_unit_test(utils_scheduler.cpp)
target_link_libraries(${test_prefix}utils_scheduler mg-utils)

add_unit_test(utils_signals.cpp)
target_link_libraries(${test_prefix}utils_signals mg-utils)

add_unit_test(utils_string.cpp)
target_link_libraries(${test_prefix}utils_string mg-utils)

add_unit_test(utils_synchronized.cpp)
target_link_libraries(${test_prefix}utils_synchronized mg-utils)

add_unit_test(utils_timestamp.cpp)
target_link_libraries(${test_prefix}utils_timestamp mg-utils)

add_unit_test(skip_list.cpp)
target_link_libraries(${test_prefix}skip_list mg-utils)

add_unit_test(small_vector.cpp)
target_link_libraries(${test_prefix}small_vector mg-utils)

add_unit_test(compact_vector.cpp)
target_link_libraries(${test_prefix}compact_vector)

add_unit_test(utils_file_locker.cpp)
target_link_libraries(${test_prefix}utils_file_locker mg-utils fmt)

add_unit_test(utils_thread_pool.cpp)
target_link_libraries(${test_prefix}utils_thread_pool mg-utils fmt)

add_unit_test(csv_csv_parsing.cpp)
target_link_libraries(${test_prefix}csv_csv_parsing mg::csv)

add_unit_test(utils_async_timer.cpp)
target_link_libraries(${test_prefix}utils_async_timer mg-utils)

add_unit_test(license.cpp)
target_link_libraries(${test_prefix}license mg-utils mg-license)

add_unit_test(utils_settings.cpp)
target_link_libraries(${test_prefix}utils_settings mg-utils mg-settings)

add_unit_test(utils_temporal utils_temporal.cpp)
target_link_libraries(${test_prefix}utils_temporal mg-utils)

add_unit_test(utils_java_string_formatter.cpp)
target_link_libraries(${test_prefix}utils_java_string_formatter mg-utils)

add_unit_test(utils_resource_lock.cpp)
target_link_libraries(${test_prefix}utils_resource_lock mg-utils)

add_unit_test(lru_cache.cpp)
target_link_libraries(${test_prefix}lru_cache mg-utils)

add_unit_test(exponential_backoff exponential_backoff.cpp)
target_link_libraries(${test_prefix}exponential_backoff mg-utils lib::rangev3)

# Test mg-storage-v2
add_unit_test(commit_log_v2.cpp)
target_link_libraries(${test_prefix}commit_log_v2 gflags mg-utils mg-storage-v2)

add_unit_test(property_value_v2.cpp)
target_link_libraries(${test_prefix}property_value_v2 mg-storage-v2 mg-utils)

add_unit_test(storage_v2.cpp)
target_link_libraries(${test_prefix}storage_v2 mg-storage-v2 storage_test_utils)

add_unit_test(storage_v2_constraints.cpp)
target_link_libraries(${test_prefix}storage_v2_constraints mg-storage-v2 mg-dbms)

add_unit_test(storage_v2_decoder_encoder.cpp)
target_link_libraries(${test_prefix}storage_v2_decoder_encoder mg-storage-v2)

add_unit_test(storage_v2_durability_inmemory.cpp)
target_link_libraries(${test_prefix}storage_v2_durability_inmemory mg-storage-v2 mg-dbms)

add_unit_test(storage_rocks.cpp)
target_link_libraries(${test_prefix}storage_rocks mg-storage-v2)

add_unit_test(storage_v2_disk.cpp)
target_link_libraries(${test_prefix}storage_v2_disk mg-storage-v2)

add_unit_test(clearing_old_disk_data.cpp)
target_link_libraries(${test_prefix}clearing_old_disk_data mg-storage-v2)

add_unit_test(storage_v2_edge_inmemory.cpp)
target_link_libraries(${test_prefix}storage_v2_edge_inmemory mg-storage-v2)

add_unit_test(storage_v2_edge_ondisk.cpp)
target_link_libraries(${test_prefix}storage_v2_edge_ondisk mg-storage-v2)

add_unit_test(storage_v2_gc.cpp)
target_link_libraries(${test_prefix}storage_v2_gc mg-storage-v2)

add_unit_test(storage_v2_indices.cpp)
target_link_libraries(${test_prefix}storage_v2_indices mg-storage-v2 mg-utils)

add_unit_test(storage_v2_name_id_mapper.cpp)
target_link_libraries(${test_prefix}storage_v2_name_id_mapper mg-storage-v2)

add_unit_test(storage_v2_property_store.cpp)
target_link_libraries(${test_prefix}storage_v2_property_store mg-storage-v2 fmt)

add_unit_test(storage_v2_wal_file.cpp)
target_link_libraries(${test_prefix}storage_v2_wal_file mg-storage-v2 storage_test_utils fmt)

add_unit_test(storage_v2_replication.cpp)
target_link_libraries(${test_prefix}storage_v2_replication mg-storage-v2 mg-dbms fmt mg-repl_coord_glue)

add_unit_test(storage_v2_isolation_level.cpp)
target_link_libraries(${test_prefix}storage_v2_isolation_level mg-storage-v2)

add_unit_test(storage_v2_show_storage_info.cpp)
target_link_libraries(${test_prefix}storage_v2_show_storage_info mg-storage-v2)

add_unit_test(storage_v2_get_info.cpp)
target_link_libraries(${test_prefix}storage_v2_get_info mg-storage-v2)

add_unit_test(database_get_info.cpp)
target_link_libraries(${test_prefix}database_get_info mg-storage-v2 mg-glue mg-dbms)

add_unit_test(storage_v2_storage_mode.cpp)
target_link_libraries(${test_prefix}storage_v2_storage_mode mg-storage-v2 storage_test_utils mg-query mg-glue)

add_unit_test(replication_persistence_helper.cpp)
target_link_libraries(${test_prefix}replication_persistence_helper mg-storage-v2 mg-repl_coord_glue)

add_unit_test(auth_checker.cpp)
target_link_libraries(${test_prefix}auth_checker mg-glue mg-auth)

add_unit_test(auth_handler.cpp)
target_link_libraries(${test_prefix}auth_handler mg-glue mg-auth)

add_unit_test(cpp_api.cpp)
target_link_libraries(${test_prefix}cpp_api mg-query)
target_include_directories(${test_prefix}cpp_api PRIVATE ${CMAKE_SOURCE_DIR}/include)

# Test mg-auth
if(MG_ENTERPRISE)
  add_unit_test(auth.cpp)
  target_link_libraries(${test_prefix}auth mg-auth mg-license)
endif()

# Test mg-slk
if(MG_ENTERPRISE)
  add_unit_test(slk_advanced.cpp)
  target_link_libraries(${test_prefix}slk_advanced mg-storage-v2 mg-replication mg-coordination mg-repl_coord_glue)
endif()

add_unit_test(slk_core.cpp)
target_link_libraries(${test_prefix}slk_core mg-slk gflags fmt)

add_unit_test(slk_streams.cpp)
target_link_libraries(${test_prefix}slk_streams mg-slk gflags fmt)

# Test mg-rpc
add_unit_test(rpc.cpp)
target_link_libraries(${test_prefix}rpc mg-rpc)

# Test websocket
find_package(Boost REQUIRED)

add_unit_test(monitoring.cpp)
target_link_libraries(${test_prefix}monitoring mg-communication Boost::headers)

# Test multi-database
if(MG_ENTERPRISE)
  add_unit_test(dbms_database.cpp)
  target_link_libraries(${test_prefix}dbms_database mg-storage-v2 mg-query mg-glue mg-dbms)

  add_unit_test_with_custom_main(dbms_handler.cpp)
  target_link_libraries(${test_prefix}dbms_handler mg-query mg-auth mg-glue mg-dbms)

  add_unit_test(multi_tenancy.cpp)
  target_link_libraries(${test_prefix}multi_tenancy mg-query mg-auth mg-glue mg-dbms)
else()
  add_unit_test_with_custom_main(dbms_handler_community.cpp)
  target_link_libraries(${test_prefix}dbms_handler_community mg-query mg-auth mg-glue mg-dbms)
endif()

# Test distributed
add_unit_test(distributed_lamport_clock.cpp)
target_link_libraries(${test_prefix}distributed_lamport_clock mg-distributed)
target_include_directories(${test_prefix}distributed_lamport_clock PRIVATE ${CMAKE_SOURCE_DIR}/include)

add_unit_test(query_hint_provider.cpp)
target_link_libraries(${test_prefix}query_hint_provider mg-query mg-glue)


# Test coordination
if(MG_ENTERPRISE)
add_unit_test(coordinator_utils.cpp)
target_link_libraries(${test_prefix}coordinator_utils gflags mg-coordination mg-repl_coord_glue)
target_include_directories(${test_prefix}coordinator_utils PRIVATE ${CMAKE_SOURCE_DIR}/include)
endif()

# Test Raft log serialization
if(MG_ENTERPRISE)
add_unit_test(coordinator_raft_log_serialization.cpp)
target_link_libraries(${test_prefix}coordinator_raft_log_serialization gflags mg-coordination mg-repl_coord_glue)
target_include_directories(${test_prefix}coordinator_raft_log_serialization PRIVATE ${CMAKE_SOURCE_DIR}/include)
endif()

# Test CoordinatorClusterState
if(MG_ENTERPRISE)
add_unit_test(coordinator_cluster_state.cpp)
target_link_libraries(${test_prefix}coordinator_cluster_state gflags mg-coordination mg-repl_coord_glue)
target_include_directories(${test_prefix}coordinator_cluster_state PRIVATE ${CMAKE_SOURCE_DIR}/include)
endif()

# Test coordinator state machine
if(MG_ENTERPRISE)
add_unit_test(coordinator_state_machine.cpp)
target_link_libraries(${test_prefix}coordinator_state_machine gflags mg-coordination mg-repl_coord_glue)
target_include_directories(${test_prefix}coordinator_state_machine PRIVATE ${CMAKE_SOURCE_DIR}/include)
endif()

if(MG_ENTERPRISE)
  add_unit_test(coordinator_state_manager.cpp)
  target_link_libraries(${test_prefix}coordinator_state_manager gflags mg-coordination mg-repl_coord_glue)
  target_include_directories(${test_prefix}coordinator_state_manager PRIVATE ${CMAKE_SOURCE_DIR}/include)
endif()

# Test RAFT state
if(MG_ENTERPRISE)
add_unit_test(coordinator_raft_state.cpp)
target_link_libraries(${test_prefix}coordinator_raft_state gflags mg-coordination mg-repl_coord_glue)
target_include_directories(${test_prefix}coordinator_raft_state PRIVATE ${CMAKE_SOURCE_DIR}/include)
endif()

# Test coordinator instance
if(MG_ENTERPRISE)
add_unit_test(coordinator_instance.cpp)
target_link_libraries(${test_prefix}coordinator_instance gflags mg-coordination mg-repl_coord_glue)
target_include_directories(${test_prefix}coordinator_instance PRIVATE ${CMAKE_SOURCE_DIR}/include)
endif()

# Test replication instance connector
if(MG_ENTERPRISE)
add_unit_test(replication_instance_connector.cpp)
target_link_libraries(${test_prefix}replication_instance_connector gflags mg-coordination mg-repl_coord_glue)
target_include_directories(${test_prefix}replication_instance_connector PRIVATE ${CMAKE_SOURCE_DIR}/include)
endif()
