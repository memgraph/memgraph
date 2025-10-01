set(test_prefix memgraph__unit__)

# Include conan-generated dependencies
include(conandeps_legacy)

find_package(fmt REQUIRED)
find_package(Threads REQUIRED)
find_package(antlr4-runtime REQUIRED)
find_package(antlr4 REQUIRED)


include_directories(${antlr4-runtime_INCLUDE_DIR})

if(NOT TARGET memgraph__unit)
    add_custom_target(memgraph__unit)
endif()

add_library(memgraph_unit_main OBJECT main.cpp)
target_link_libraries(memgraph_unit_main mg-memory mg-utils GTest::gtest GTest::gmock Threads::Threads dl atomic)
target_precompile_headers(memgraph_unit_main PRIVATE
    <gtest/gtest.h>
)



# Test utilities
add_library(storage_test_utils storage_test_utils.cpp)
target_link_libraries(storage_test_utils mg::storage)

find_package(OpenSSL REQUIRED)
find_package(Boost REQUIRED CONFIG COMPONENTS filesystem)

# Test integrations-kafka
add_library(kafka-mock STATIC kafka_mock.cpp)
target_link_libraries(kafka-mock mg-utils librdkafka++ librdkafka Threads::Threads GTest::gtest)

# Include directories are intentionally not set, because kafka-mock isn't meant to be used apart from unit tests
add_unit_test(integrations_kafka_consumer
    SOURCES integrations_kafka_consumer.cpp kafka_mock.cpp
    LINK_TARGETS kafka-mock mg-integrations-kafka
)

add_unit_test(mgp_kafka_c_api
    SOURCES mgp_kafka_c_api.cpp
    LINK_TARGETS mg-query mg-integrations-kafka
)

add_unit_test(mgp_trans_c_api
    SOURCES mgp_trans_c_api.cpp
    LINK_TARGETS mg-query
)

# Test mg-query
add_unit_test(bfs_single_node
    SOURCES bfs_single_node.cpp
    LINK_TARGETS mg-query mg-glue
)

add_unit_test(bfs_fine_grained
    SOURCES bfs_fine_grained.cpp
    LINK_TARGETS mg-query mg-glue
)

add_unit_test(kshortest_fine_grained
    SOURCES kshortest_fine_grained.cpp
    LINK_TARGETS mg-query mg-glue
)

add_unit_test(kshortest_general
    SOURCES kshortest_general.cpp
    LINK_TARGETS mg-query mg-glue
)

add_unit_test(cypher_main_visitor
    SOURCES cypher_main_visitor.cpp
    LINK_TARGETS mg-query
)

add_unit_test(event_map
        SOURCES event_map.cpp
        LINK_TARGETS mg-events mg-utils
)


add_unit_test(interpreter
    SOURCES interpreter.cpp ${CMAKE_SOURCE_DIR}/src/glue/communication.cpp
    LINK_TARGETS mg-communication mg-query mg-glue
)

add_unit_test(plan_pretty_print
    SOURCES plan_pretty_print.cpp
    LINK_TARGETS mg-query
)

add_unit_test(query_cost_estimator
    SOURCES query_cost_estimator.cpp
    LINK_TARGETS mg-query
)

add_unit_test(query_dump
    SOURCES query_dump.cpp ${CMAKE_SOURCE_DIR}/src/glue/communication.cpp
    LINK_TARGETS mg-communication mg-query
)

add_unit_test(query_expression_evaluator
    SOURCES query_expression_evaluator.cpp
    LINK_TARGETS mg-query
)

add_unit_test(query_plan
    SOURCES query_plan.cpp
    LINK_TARGETS mg-query
)

add_unit_test(query_plan_accumulate_aggregate
    SOURCES query_plan_accumulate_aggregate.cpp
    LINK_TARGETS mg-query mg-glue
)

add_unit_test(query_plan_bag_semantics
    SOURCES query_plan_bag_semantics.cpp
    LINK_TARGETS mg-query mg-glue
)

add_unit_test(query_plan_create_set_remove_delete
    SOURCES query_plan_create_set_remove_delete.cpp
    LINK_TARGETS mg-query mg-glue
)

add_unit_test(query_plan_edge_cases
    SOURCES query_plan_edge_cases.cpp ${CMAKE_SOURCE_DIR}/src/glue/communication.cpp
    LINK_TARGETS mg-communication mg-query
)

add_unit_test(query_plan_match_filter_return
    SOURCES query_plan_match_filter_return.cpp
    LINK_TARGETS mg-query mg-glue
)

add_unit_test(query_plan_operator_to_string
    SOURCES query_plan_operator_to_string.cpp
    LINK_TARGETS mg-query
)

add_unit_test(query_plan_read_write_typecheck
    SOURCES query_plan_read_write_typecheck.cpp ${CMAKE_SOURCE_DIR}/src/query/plan/read_write_type_checker.cpp
    LINK_TARGETS mg-query
)

add_unit_test(query_plan_v2_create_set_remove_delete
    SOURCES query_plan_v2_create_set_remove_delete.cpp
    LINK_TARGETS mg-query mg-glue
)

add_unit_test(query_pretty_print
    SOURCES query_pretty_print.cpp
    LINK_TARGETS mg-query
)

add_unit_test(query_trigger
    SOURCES query_trigger.cpp
    LINK_TARGETS mg-query mg-glue
)

add_unit_test(query_serialization_property_value
    SOURCES query_serialization_property_value.cpp
    LINK_TARGETS mg-query
)

add_unit_test(query_streams
    SOURCES query_streams.cpp
    LINK_TARGETS mg-query kafka-mock
)

add_unit_test(transaction_queue
    SOURCES transaction_queue.cpp
    LINK_TARGETS mg-communication mg-query mg-glue
)

add_unit_test(transaction_queue_multiple
    SOURCES transaction_queue_multiple.cpp
    LINK_TARGETS mg-communication mg-query mg-glue
)

# Test query functions
add_unit_test(query_function_mgp_module
    SOURCES query_function_mgp_module.cpp
    LINK_TARGETS mg-query
    INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/include
)


# Test query/procedure
add_unit_test(query_procedure_mgp_type
    SOURCES query_procedure_mgp_type.cpp
    LINK_TARGETS mg-query
    INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/include
)


add_unit_test(query_procedure_mgp_module
    SOURCES query_procedure_mgp_module.cpp
    LINK_TARGETS mg-query
    INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/include
)


add_unit_test(query_procedure_py_module
    SOURCES query_procedure_py_module.cpp
    LINK_TARGETS mg-query
    CUSTOM_MAIN
    INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/include
)


add_unit_test(query_procedures_mgp_graph
    SOURCES query_procedures_mgp_graph.cpp
    LINK_TARGETS mg-query storage_test_utils
    INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/include
)


# END query/procedure
add_unit_test(query_profile
    SOURCES query_profile.cpp
    LINK_TARGETS mg-query
)

add_unit_test(query_required_privileges
    SOURCES query_required_privileges.cpp
    LINK_TARGETS mg-query
)

add_unit_test(query_semantic
    SOURCES query_semantic.cpp
    LINK_TARGETS mg-query
)

add_unit_test(query_variable_start_planner
    SOURCES query_variable_start_planner.cpp
    LINK_TARGETS mg-query mg-glue
)

add_unit_test(stripped
    SOURCES stripped.cpp
    LINK_TARGETS mg-query
)

add_unit_test(typed_value
    SOURCES typed_value.cpp
    LINK_TARGETS mg-query
)

# Test mg-communication
add_unit_test(bolt_chunked_decoder_buffer
    SOURCES bolt_chunked_decoder_buffer.cpp
    LINK_TARGETS mg-communication
)

add_unit_test(bolt_chunked_encoder_buffer
    SOURCES bolt_chunked_encoder_buffer.cpp
    LINK_TARGETS mg-communication
)

add_unit_test(bolt_decoder
    SOURCES bolt_decoder.cpp
    LINK_TARGETS mg-communication
)

add_unit_test(bolt_encoder
    SOURCES bolt_encoder.cpp ${CMAKE_SOURCE_DIR}/src/glue/communication.cpp
    LINK_TARGETS mg-communication mg-query
)

add_unit_test(bolt_session
    SOURCES bolt_session.cpp
    LINK_TARGETS mg-communication mg-utils
)

add_unit_test(communication_buffer
    SOURCES communication_buffer.cpp
    LINK_TARGETS mg-communication mg-utils
)

add_unit_test(network_timeouts
    SOURCES network_timeouts.cpp
    LINK_TARGETS mg-communication
)

# Test mg-kvstore
add_unit_test(kvstore
    SOURCES kvstore.cpp
    LINK_TARGETS mg-kvstore mg-utils
)

# Test data structures
add_unit_test(ring_buffer
    SOURCES ring_buffer.cpp
    LINK_TARGETS mg-utils
)

# Test mg-io
add_unit_test(network_endpoint
    SOURCES network_endpoint.cpp
    LINK_TARGETS mg-io
)

add_unit_test(network_utils
    SOURCES network_utils.cpp
    LINK_TARGETS mg-io mg-utils
)

add_unit_test(socket
    SOURCES socket.cpp
    LINK_TARGETS mg-utils mg-io mg-events
)

# Test mg-utils
add_unit_test(utils_algorithm
    SOURCES utils_algorithm.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(utils_barrier
    SOURCES utils_barrier.cpp
    LINK_TARGETS mg-utils
    TEST_PROPERTIES TIMEOUT 60
)

add_unit_test(utils_hot_mask
    SOURCES utils_hot_mask.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(utils_priority_thread_pool
    SOURCES utils_priority_thread_pool.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(utils_exceptions
    SOURCES utils_exceptions.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(utils_histogram
    SOURCES utils_histogram.cpp
    LINK_TARGETS mg-utils mg-events
)

add_unit_test(utils_file
    SOURCES utils_file.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(utils_math
    SOURCES utils_math.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(utils_memory
    SOURCES utils_memory.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(utils_memory_tracker
    SOURCES utils_memory_tracker.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(utils_query_memory_tracker
    SOURCES utils_query_memory_tracker.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(utils_on_scope_exit
    SOURCES utils_on_scope_exit.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(utils_rwlock
    SOURCES utils_rwlock.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(utils_scheduler
    SOURCES utils_scheduler.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(utils_signals
    SOURCES utils_signals.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(utils_string
    SOURCES utils_string.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(utils_synchronized
    SOURCES utils_synchronized.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(utils_timestamp
    SOURCES utils_timestamp.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(skip_list
    SOURCES skip_list.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(small_vector
    SOURCES small_vector.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(utils_mask_sensitive_information
    SOURCES utils_mask_sensitive_information.cpp
    LINK_TARGETS mg-utils fmt::fmt
)

add_unit_test(utils_file_locker
    SOURCES utils_file_locker.cpp
    LINK_TARGETS mg-utils fmt::fmt
)

add_unit_test(utils_thread_pool
    SOURCES utils_thread_pool.cpp
    LINK_TARGETS mg-utils fmt::fmt
)

add_unit_test(csv_csv_parsing
    SOURCES csv_csv_parsing.cpp
    LINK_TARGETS mg::csv
)

add_unit_test(utils_async_timer
    SOURCES utils_async_timer.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(license
    SOURCES license.cpp
    LINK_TARGETS mg-utils mg-license
)

add_unit_test(utils_settings
    SOURCES utils_settings.cpp
    LINK_TARGETS mg-utils mg-settings
)

add_unit_test(utils_temporal
    SOURCES utils_temporal.cpp
    LINK_TARGETS mg-utils mg-flags
)

add_unit_test(utils_java_string_formatter
    SOURCES utils_java_string_formatter.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(utils_resource_lock
    SOURCES utils_resource_lock.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(lru_cache
    SOURCES lru_cache.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(utils_static_vector
    SOURCES utils_static_vector.cpp
    LINK_TARGETS mg::utils
)

add_unit_test(exponential_backoff
    SOURCES exponential_backoff.cpp
    LINK_TARGETS mg-utils range-v3::range-v3
)

add_unit_test(utils_compressor
    SOURCES utils_compressor.cpp
    LINK_TARGETS mg-utils
)

add_unit_test(file_replication_handler
    SOURCES file_replication_handler.cpp
    LINK_TARGETS mg-rpc mg-slk
)

add_unit_test(utils_observer
    SOURCES utils_observer.cpp
    LINK_TARGETS mg-utils
)

# Test mg::storage
add_unit_test(commit_log_v2
    SOURCES commit_log_v2.cpp
    LINK_TARGETS gflags mg-utils mg::storage
)

add_unit_test(property_value_v2
    SOURCES property_value_v2.cpp
    LINK_TARGETS mg::storage mg-utils
)

add_unit_test(storage_v2
    SOURCES storage_v2.cpp
    LINK_TARGETS mg::storage storage_test_utils
)

add_unit_test(storage_v2_acc
    SOURCES storage_v2_acc.cpp
    LINK_TARGETS mg::storage
)

add_unit_test(storage_v2_delta_container
    SOURCES storage_v2_delta_container.cpp
    LINK_TARGETS mg::storage
)

add_unit_test(storage_v2_enum_store
    SOURCES storage_v2_enum_store.cpp
    LINK_TARGETS mg::storage
)

add_unit_test(storage_v2_constraints
    SOURCES storage_v2_constraints.cpp
    LINK_TARGETS mg::storage mg-dbms
)

add_unit_test(storage_v2_decoder_encoder
    SOURCES storage_v2_decoder_encoder.cpp
    LINK_TARGETS mg::storage
)

add_unit_test(storage_v2_durability_inmemory
    SOURCES storage_v2_durability_inmemory.cpp
    LINK_TARGETS mg::storage mg-dbms
)

add_unit_test(storage_v2_create_snapshot
    SOURCES storage_v2_create_snapshot.cpp
    LINK_TARGETS mg::storage mg-dbms
)

add_unit_test(storage_v2_retention.
        SOURCES storage_v2_retention.cpp
        LINK_TARGETS mg::storage
)

add_unit_test(storage_rocks
    SOURCES storage_rocks.cpp
    LINK_TARGETS mg::storage
)

add_unit_test(storage_v2_disk
    SOURCES storage_v2_disk.cpp
    LINK_TARGETS mg::storage
)

add_unit_test(clearing_old_disk_data
    SOURCES clearing_old_disk_data.cpp
    LINK_TARGETS mg::storage
)

add_unit_test(storage_v2_edge_inmemory
    SOURCES storage_v2_edge_inmemory.cpp
    LINK_TARGETS mg::storage
)

add_unit_test(storage_v2_edge_ondisk
    SOURCES storage_v2_edge_ondisk.cpp
    LINK_TARGETS mg::storage
)

add_unit_test(storage_v2_gc
    SOURCES storage_v2_gc.cpp
    LINK_TARGETS mg::storage
)

add_unit_test(storage_v2_indices
    SOURCES storage_v2_indices.cpp
    LINK_TARGETS mg::storage mg-utils
)

add_unit_test(storage_v2_name_id_mapper
    SOURCES storage_v2_name_id_mapper.cpp
    LINK_TARGETS mg::storage
)

add_unit_test(storage_v2_property_store
    SOURCES storage_v2_property_store.cpp
    LINK_TARGETS mg::storage
    CUSTOM_MAIN
)

add_unit_test(storage_v2_wal_file
    SOURCES storage_v2_wal_file.cpp
    LINK_TARGETS mg::storage storage_test_utils fmt::fmt
)

add_unit_test(storage_v2_replication
    SOURCES storage_v2_replication.cpp
    LINK_TARGETS mg::storage mg-dbms fmt::fmt mg-repl_coord_glue
)

add_unit_test(storage_v2_isolation_level
    SOURCES storage_v2_isolation_level.cpp
    LINK_TARGETS mg::storage
)

add_unit_test(storage_v2_show_storage_info
    SOURCES storage_v2_show_storage_info.cpp
    LINK_TARGETS mg::storage
)

add_unit_test(storage_v2_get_info
    SOURCES storage_v2_get_info.cpp
    LINK_TARGETS mg::storage
)

add_unit_test(database_get_info
    SOURCES database_get_info.cpp
    LINK_TARGETS mg::storage mg-glue mg-dbms
)

add_unit_test(storage_v2_storage_mode
    SOURCES storage_v2_storage_mode.cpp
    LINK_TARGETS mg::storage storage_test_utils mg-query mg-glue
)

add_unit_test(storage_v2_schema_info
    SOURCES storage_v2_schema_info.cpp
    LINK_TARGETS mg::storage
)

add_unit_test(replication_persistence_helper
    SOURCES replication_persistence_helper.cpp
    LINK_TARGETS mg::storage mg-repl_coord_glue
)

add_unit_test(auth_checker
    SOURCES auth_checker.cpp
    LINK_TARGETS mg-glue mg-auth
)

add_unit_test(auth_handler
    SOURCES auth_handler.cpp
    LINK_TARGETS mg-glue mg-auth
)

if (MG_ENTERPRISE)
    add_unit_test(auth_models
        SOURCES auth_models.cpp
        LINK_TARGETS mg-glue mg-auth
    )
else ()
    add_unit_test(auth_models_comm
        SOURCES auth_models_comm.cpp
        LINK_TARGETS mg-glue mg-auth
    )
endif ()

add_unit_test(cpp_api
    SOURCES cpp_api.cpp
    LINK_TARGETS mg-query
    INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/include
)


add_unit_test(vector_index
    SOURCES vector_index.cpp
    LINK_TARGETS mg::storage
)

add_unit_test(vector_edge_index
    SOURCES vector_edge_index.cpp
    LINK_TARGETS mg::storage
)

add_unit_test(text_index
    SOURCES text_index.cpp
    LINK_TARGETS mg::storage
)

# Test mg-auth
if (MG_ENTERPRISE)
    add_unit_test(auth
        SOURCES auth.cpp
        LINK_TARGETS mg-auth mg-license Boost::filesystem
    )

    add_custom_target(memgraph__unit__auth_kvstore_dir ALL
        COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_CURRENT_SOURCE_DIR}/auth_kvstore
        ${CMAKE_CURRENT_BINARY_DIR}/auth_kvstore
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/auth_kvstore)
    add_dependencies(memgraph__unit memgraph__unit__auth_kvstore_dir)
    add_dependencies(${test_unit_prefix}auth memgraph__unit__auth_kvstore_dir)
endif ()

# Test mg-slk
if (MG_ENTERPRISE)
    add_unit_test(slk_advanced
        SOURCES slk_advanced.cpp
        LINK_TARGETS mg::storage mg-replication mg-coordination mg-repl_coord_glue
    )
endif ()

add_unit_test(slk_core
    SOURCES slk_core.cpp
    LINK_TARGETS mg-slk gflags fmt::fmt
)

add_unit_test(slk_streams
    SOURCES slk_streams.cpp
    LINK_TARGETS mg-slk gflags fmt::fmt
)

# Test utilities
add_library(rpc_messages rpc_messages.cpp)
target_link_libraries(rpc_messages mg-rpc mg-slk)

# Test mg-rpc
add_unit_test(rpc
    SOURCES rpc.cpp
    LINK_TARGETS mg-rpc rpc_messages
)

# Test rpc-timeouts
if (MG_ENTERPRISE)
    add_unit_test(rpc_timeouts
        SOURCES rpc_timeouts.cpp
        LINK_TARGETS mg-rpc mg-coordination mg-replication mg-repl_coord_glue rpc_messages
    )
endif ()

# Test rpc-in-progress
add_unit_test(rpc_in_progress
    SOURCES rpc_in_progress.cpp
    LINK_TARGETS mg-rpc rpc_messages
)

# Test replication-rpc-progress
add_unit_test(replication_rpc_progress
    SOURCES replication_rpc_progress.cpp
    LINK_TARGETS mg-rpc rpc_messages
)

# Test snapshot-rpc-progress
add_unit_test(snapshot_rpc_progress
    SOURCES snapshot_rpc_progress.cpp
    LINK_TARGETS mg-rpc rpc_messages
)

add_unit_test(rpc_versioning
    SOURCES rpc_versioning.cpp
    LINK_TARGETS mg-coordination mg-repl_coord_glue mg-rpc rpc_messages
)

# Test websocket

add_unit_test(monitoring
    SOURCES monitoring.cpp
    LINK_TARGETS mg-communication Boost::headers
)

# Test multi-database
if (MG_ENTERPRISE)
    add_unit_test(dbms_database
        SOURCES dbms_database.cpp
        LINK_TARGETS mg::storage mg-query mg-glue mg-dbms
    )

    add_unit_test(dbms_handler
        SOURCES dbms_handler.cpp
        LINK_TARGETS mg::storage mg-query mg-glue mg-dbms
        CUSTOM_MAIN
    )

    add_unit_test(multi_tenancy
        SOURCES multi_tenancy.cpp
        LINK_TARGETS mg-query mg-auth mg-glue mg-dbms
    )
else ()
    add_unit_test(dbms_handler_community
        SOURCES dbms_handler_community.cpp
        LINK_TARGETS mg::storage mg-query mg-glue mg-dbms
        CUSTOM_MAIN
    )
endif ()

# Test distributed
add_unit_test(distributed_lamport_clock
    SOURCES distributed_lamport_clock.cpp
    LINK_TARGETS mg-distributed
    INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/include
)

add_unit_test(query_hint_provider
    SOURCES query_hint_provider.cpp
    LINK_TARGETS mg-query mg-glue
)


add_unit_test(counter
    SOURCES counter.cpp
    LINK_TARGETS mg-utils
)

# Test coordination
if (MG_ENTERPRISE)
    add_unit_test(coordinator_utils
        SOURCES coordinator_utils.cpp
        LINK_TARGETS gflags mg-coordination mg-repl_coord_glue
        INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/include
    )

endif ()

# Test Raft log serialization
if (MG_ENTERPRISE)
    add_unit_test(coordinator_raft_log_serialization
        SOURCES coordinator_raft_log_serialization.cpp
        LINK_TARGETS gflags mg-coordination mg-repl_coord_glue
        INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/include
    )

endif ()

# Test CoordinatorClusterState
if (MG_ENTERPRISE)
    add_unit_test(coordinator_cluster_state
        SOURCES coordinator_cluster_state.cpp
        LINK_TARGETS gflags mg-coordination mg-repl_coord_glue
        INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/include
    )

endif ()

# Test coordinator state machine
if (MG_ENTERPRISE)
    add_unit_test(coordinator_state_machine
        SOURCES coordinator_state_machine.cpp
        LINK_TARGETS gflags mg-coordination mg-repl_coord_glue
        INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/include
    )

endif ()

if (MG_ENTERPRISE)
    add_unit_test(coordinator_state_manager
        SOURCES coordinator_state_manager.cpp
        LINK_TARGETS gflags mg-coordination mg-repl_coord_glue
        INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/include
    )

endif ()

if (MG_ENTERPRISE)
    add_unit_test(coordinator_log_store
        SOURCES coordinator_log_store.cpp
        LINK_TARGETS gflags mg-coordination mg-repl_coord_glue
        INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/include
    )

endif ()

# Test RAFT state
if (MG_ENTERPRISE)
    add_unit_test(coordinator_raft_state
        SOURCES coordinator_raft_state.cpp
        LINK_TARGETS gflags mg-coordination mg-repl_coord_glue
        INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/include
    )

endif ()

# Test coordinator env flag handler
if (MG_ENTERPRISE)
    add_unit_test(coordinator_env_flag_handler
        SOURCES coordinator_env_flag_handler.cpp
        LINK_TARGETS gflags mg-flags
        INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/include
    )

endif ()

# Test coordinator instance context
if (MG_ENTERPRISE)
    add_unit_test(coordinator_instance_context
        SOURCES coordinator_instance_context.cpp
        LINK_TARGETS gflags mg-coordination
        INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/include
    )

endif ()

# Test coordinator instance aux
if (MG_ENTERPRISE)
    add_unit_test(coordinator_instance_aux
        SOURCES coordinator_instance_aux.cpp
        LINK_TARGETS gflags mg-coordination
        INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/include
    )

endif ()

# Test data instance context
if (MG_ENTERPRISE)
    add_unit_test(data_instance_context
        SOURCES data_instance_context.cpp
        LINK_TARGETS gflags mg-coordination
        INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/include
    )

endif ()

# Build all HA unit tests
if (MG_ENTERPRISE)
    add_custom_target(memgraph__unit__high_availability ALL
        DEPENDS memgraph__unit__coordinator_utils
        memgraph__unit__coordinator_raft_log_serialization
        memgraph__unit__coordinator_cluster_state
        memgraph__unit__coordinator_state_machine
        memgraph__unit__coordinator_state_manager
        memgraph__unit__coordinator_log_store
        memgraph__unit__coordinator_raft_state
        memgraph__unit__coordinator_env_flag_handler
        memgraph__unit__coordinator_instance_context
        memgraph__unit__coordinator_instance_aux
    )

    add_custom_target(run_ha_tests
        COMMAND ctest -j4 -R memgraph__unit__coordinator_
    )

endif ()

# Test time to live
if (MG_ENTERPRISE)
    add_unit_test(ttl
        SOURCES ttl.cpp
        LINK_TARGETS mg-query
        INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/include
    )

endif ()

# Test database protector factory system
add_unit_test(database_protector_test
    SOURCES database_protector_test.cpp
    LINK_TARGETS mg-storage-v2
)

# Test community detection online
# avoid linking with shared library due to missing symbols
if (MG_ENTERPRISE)
    add_unit_test(community_detection_online
        SOURCES community_detection_online.cpp ${CMAKE_SOURCE_DIR}/query_modules/community_detection_module/algorithm_online/community_detection.cpp
        INCLUDE_DIRS
        ${CMAKE_SOURCE_DIR}/query_modules/community_detection_module
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/query_modules/mg_utility
    )
endif ()

# Test katz centrality online
# avoid linking with shared library due to missing symbols
if (MG_ENTERPRISE)
    add_unit_test(katz_centrality_online
        SOURCES katz_centrality_online.cpp ${CMAKE_SOURCE_DIR}/query_modules/katz_centrality_module/algorithm/katz.cpp
        INCLUDE_DIRS
        ${CMAKE_SOURCE_DIR}/query_modules/katz_centrality_module
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/query_modules/mg_utility
    )
endif ()
