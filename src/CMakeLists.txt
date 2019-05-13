# CMake configuration for the main memgraph library and executable

# add memgraph sub libraries, ordered by dependency
add_subdirectory(lisp)
add_subdirectory(utils)
add_subdirectory(requests)
add_subdirectory(integrations)
add_subdirectory(io)
add_subdirectory(telemetry)
add_subdirectory(communication)
add_subdirectory(stats)
add_subdirectory(auth)
add_subdirectory(slk)

# ----------------------------------------------------------------------------
# Common LCP files
# ----------------------------------------------------------------------------

define_add_lcp(add_lcp_common lcp_common_cpp_files generated_lcp_common_files)

add_lcp_common(query/frontend/ast/ast.lcp)
add_lcp_common(query/frontend/semantic/symbol.lcp)
add_lcp_common(query/plan/operator.lcp)

add_custom_target(generate_lcp_common DEPENDS ${generated_lcp_common_files})

# ----------------------------------------------------------------------------
# END Common LCP files
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# Memgraph Single Node
# ----------------------------------------------------------------------------
set(mg_single_node_sources
    ${lcp_common_cpp_files}
    audit/log.cpp
    data_structures/concurrent/skiplist_gc.cpp
    database/single_node/config.cpp
    database/single_node/dump.cpp
    database/single_node/graph_db.cpp
    database/single_node/graph_db_accessor.cpp
    durability/single_node/state_delta.cpp
    durability/single_node/paths.cpp
    durability/single_node/recovery.cpp
    durability/single_node/snapshooter.cpp
    durability/single_node/wal.cpp
    glue/auth.cpp
    glue/communication.cpp
    query/common.cpp
    query/frontend/ast/cypher_main_visitor.cpp
    query/frontend/ast/pretty_print.cpp
    query/frontend/semantic/required_privileges.cpp
    query/frontend/semantic/symbol_generator.cpp
    query/frontend/stripped.cpp
    query/interpret/awesome_memgraph_functions.cpp
    query/interpreter.cpp
    query/plan/operator.cpp
    query/plan/preprocess.cpp
    query/plan/pretty_print.cpp
    query/plan/profile.cpp
    query/plan/rewrite/index_lookup.cpp
    query/plan/rule_based_planner.cpp
    query/plan/variable_start_planner.cpp
    query/repl.cpp
    query/typed_value.cpp
    storage/single_node/edge_accessor.cpp
    storage/common/locking/record_lock.cpp
    storage/common/types/property_value.cpp
    storage/common/types/property_value_store.cpp
    storage/single_node/record_accessor.cpp
    storage/single_node/vertex_accessor.cpp
    storage/single_node/constraints/record.cpp
		storage/single_node/constraints/unique_constraints.cpp
    transactions/single_node/engine.cpp
    memgraph_init.cpp
)

define_add_lcp(add_lcp_single_node mg_single_node_sources generated_lcp_single_node_files)

add_lcp_single_node(durability/single_node/state_delta.lcp)

add_custom_target(generate_lcp_single_node DEPENDS generate_lcp_common ${generated_lcp_single_node_files})

set(MG_SINGLE_NODE_LIBS stdc++fs Threads::Threads fmt cppitertools
    antlr_opencypher_parser_lib dl glog gflags
    mg-utils mg-io mg-requests mg-communication)
# These are enterprise subsystems
set(MG_SINGLE_NODE_LIBS ${MG_SINGLE_NODE_LIBS} mg-integrations-kafka mg-auth)

if (USE_LTALLOC)
  list(APPEND MG_SINGLE_NODE_LIBS ltalloc)
  # TODO(mferencevic): Enable this when clang is updated on apollo.
  # set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -flto")
endif()

if (READLINE_FOUND)
  list(APPEND MG_SINGLE_NODE_LIBS readline)
endif()

add_library(mg-single-node STATIC ${mg_single_node_sources})
target_link_libraries(mg-single-node ${MG_SINGLE_NODE_LIBS})
add_dependencies(mg-single-node generate_opencypher_parser)
add_dependencies(mg-single-node generate_lcp_single_node)
target_compile_definitions(mg-single-node PUBLIC MG_SINGLE_NODE)

# ----------------------------------------------------------------------------
# END Memgraph Single Node
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# Memgraph Distributed
# ----------------------------------------------------------------------------

set(mg_distributed_sources
    ${lcp_common_cpp_files}
    audit/log.cpp
    database/distributed/distributed_graph_db.cpp
    distributed/bfs_rpc_clients.cpp
    distributed/bfs_subcursor.cpp
    distributed/cluster_discovery_master.cpp
    distributed/cluster_discovery_worker.cpp
    distributed/coordination.cpp
    distributed/coordination_master.cpp
    distributed/coordination_worker.cpp
    distributed/data_manager.cpp
    distributed/data_rpc_clients.cpp
    distributed/data_rpc_server.cpp
    distributed/dgp/partitioner.cpp
    distributed/dgp/vertex_migrator.cpp
    distributed/durability_rpc_master.cpp
    distributed/durability_rpc_worker.cpp
    distributed/dynamic_worker.cpp
    distributed/index_rpc_server.cpp
    distributed/plan_consumer.cpp
    distributed/plan_dispatcher.cpp
    distributed/produce_rpc_server.cpp
    distributed/pull_rpc_clients.cpp
    distributed/updates_rpc_clients.cpp
    distributed/updates_rpc_server.cpp
    query/distributed/interpreter.cpp
    query/distributed/plan/planner.cpp
    query/distributed/plan/ops.cpp
    query/distributed/plan/pretty_print.cpp
    storage/distributed/concurrent_id_mapper_master.cpp
    storage/distributed/concurrent_id_mapper_worker.cpp
    transactions/distributed/engine_master.cpp
    transactions/distributed/engine_worker.cpp
    data_structures/concurrent/skiplist_gc.cpp
    database/distributed/config.cpp
    database/distributed/graph_db_accessor.cpp
    durability/distributed/state_delta.cpp
    durability/distributed/paths.cpp
    durability/distributed/recovery.cpp
    durability/distributed/snapshooter.cpp
    durability/distributed/wal.cpp
    glue/auth.cpp
    glue/communication.cpp
    query/common.cpp
    query/frontend/ast/pretty_print.cpp
    query/frontend/ast/cypher_main_visitor.cpp
    query/frontend/semantic/required_privileges.cpp
    query/frontend/semantic/symbol_generator.cpp
    query/frontend/stripped.cpp
    query/interpret/awesome_memgraph_functions.cpp
    query/interpreter.cpp
    query/plan/operator.cpp
    query/plan/preprocess.cpp
    query/plan/pretty_print.cpp
    query/plan/profile.cpp
    query/plan/rewrite/index_lookup.cpp
    query/plan/rule_based_planner.cpp
    query/plan/variable_start_planner.cpp
    query/repl.cpp
    query/distributed/serialization.cpp
    query/typed_value.cpp
    storage/common/locking/record_lock.cpp
    storage/common/types/property_value.cpp
    storage/common/types/property_value_store.cpp
    storage/common/types/slk.cpp
    storage/distributed/edge_accessor.cpp
    storage/distributed/edges_iterator.cpp
    storage/distributed/record_accessor.cpp
    storage/distributed/rpc/serialization.cpp
    storage/distributed/vertex_accessor.cpp
    memgraph_init.cpp
    transactions/distributed/engine_single_node.cpp
)

# -----------------------------------------------------------------------------

define_add_lcp(add_lcp_distributed mg_distributed_sources generated_lcp_distributed_files)

add_lcp_distributed(durability/distributed/state_delta.lcp)
add_lcp_distributed(database/distributed/serialization.lcp SLK_SERIALIZE
                    DEPENDS durability/distributed/state_delta.lcp)
add_lcp_distributed(distributed/bfs_rpc_messages.lcp SLK_SERIALIZE)
add_lcp_distributed(distributed/coordination_rpc_messages.lcp SLK_SERIALIZE)
add_lcp_distributed(distributed/data_rpc_messages.lcp SLK_SERIALIZE)
add_lcp_distributed(distributed/durability_rpc_messages.lcp SLK_SERIALIZE)
add_lcp_distributed(distributed/index_rpc_messages.lcp SLK_SERIALIZE)
add_lcp_distributed(distributed/plan_rpc_messages.lcp SLK_SERIALIZE)
add_lcp_distributed(distributed/pull_produce_rpc_messages.lcp SLK_SERIALIZE
                    DEPENDS transactions/distributed/serialization.lcp)
add_lcp_distributed(distributed/storage_gc_rpc_messages.lcp SLK_SERIALIZE)
add_lcp_distributed(distributed/token_sharing_rpc_messages.lcp SLK_SERIALIZE)
add_lcp_distributed(distributed/updates_rpc_messages.lcp SLK_SERIALIZE)
add_lcp_distributed(distributed/dynamic_worker_rpc_messages.lcp SLK_SERIALIZE)

add_lcp_distributed(query/distributed/frontend/ast/ast_serialization.lcp SLK_SERIALIZE
        DEPENDS query/frontend/ast/ast.lcp)
add_lcp_distributed(query/distributed/frontend/semantic/symbol_serialization.lcp SLK_SERIALIZE
        DEPENDS query/frontend/semantic/symbol.lcp)
add_lcp_distributed(query/distributed/plan/ops.lcp SLK_SERIALIZE
        DEPENDS query/plan/operator.lcp)

add_lcp_distributed(storage/distributed/rpc/concurrent_id_mapper_rpc_messages.lcp SLK_SERIALIZE)
add_lcp_distributed(transactions/distributed/engine_rpc_messages.lcp SLK_SERIALIZE
        DEPENDS transactions/distributed/serialization.lcp)

add_custom_target(generate_lcp_distributed DEPENDS generate_lcp_common ${generated_lcp_distributed_files})

set(MG_DISTRIBUTED_LIBS stdc++fs Threads::Threads fmt cppitertools
    antlr_opencypher_parser_lib dl glog gflags
    mg-utils mg-io mg-integrations-kafka mg-requests
    mg-communication mg-comm-rpc mg-auth)

# STATIC library used by memgraph executables
add_library(mg-distributed STATIC ${mg_distributed_sources})
target_link_libraries(mg-distributed ${MG_DISTRIBUTED_LIBS})
add_dependencies(mg-distributed generate_opencypher_parser)
add_dependencies(mg-distributed generate_lcp_distributed)
target_compile_definitions(mg-distributed PUBLIC MG_DISTRIBUTED)

# ----------------------------------------------------------------------------
# END Memgraph Distributed
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# Memgraph Single Node High Availability
# ----------------------------------------------------------------------------
set(mg_single_node_ha_sources
    ${lcp_common_cpp_files}
    audit/log.cpp
    data_structures/concurrent/skiplist_gc.cpp
    database/single_node_ha/config.cpp
    database/single_node_ha/graph_db.cpp
    database/single_node_ha/graph_db_accessor.cpp
    durability/single_node_ha/state_delta.cpp
    durability/single_node_ha/paths.cpp
    durability/single_node_ha/snapshooter.cpp
    durability/single_node_ha/recovery.cpp
    glue/auth.cpp
    glue/communication.cpp
    raft/coordination.cpp
    raft/raft_server.cpp
    raft/storage_info.cpp
    query/common.cpp
    query/frontend/ast/cypher_main_visitor.cpp
    query/frontend/ast/pretty_print.cpp
    query/frontend/semantic/required_privileges.cpp
    query/frontend/semantic/symbol_generator.cpp
    query/frontend/stripped.cpp
    query/interpret/awesome_memgraph_functions.cpp
    query/interpreter.cpp
    query/plan/operator.cpp
    query/plan/preprocess.cpp
    query/plan/pretty_print.cpp
    query/plan/profile.cpp
    query/plan/rewrite/index_lookup.cpp
    query/plan/rule_based_planner.cpp
    query/plan/variable_start_planner.cpp
    query/repl.cpp
    query/typed_value.cpp
    storage/common/types/property_value.cpp
    storage/common/types/slk.cpp
    storage/common/types/property_value_store.cpp
    storage/common/locking/record_lock.cpp
    storage/single_node_ha/edge_accessor.cpp
    storage/single_node_ha/record_accessor.cpp
    storage/single_node_ha/vertex_accessor.cpp
    transactions/single_node_ha/engine.cpp
    memgraph_init.cpp
)

define_add_lcp(add_lcp_single_node_ha mg_single_node_ha_sources generated_lcp_single_node_ha_files)

add_lcp_single_node_ha(durability/single_node_ha/state_delta.lcp)
add_lcp_single_node_ha(database/single_node_ha/serialization.lcp SLK_SERIALIZE
        DEPENDS durability/single_node_ha/state_delta.lcp)
add_lcp_single_node_ha(raft/raft_rpc_messages.lcp SLK_SERIALIZE)
add_lcp_single_node_ha(raft/log_entry.lcp SLK_SERIALIZE)
add_lcp_single_node_ha(raft/snapshot_metadata.lcp SLK_SERIALIZE)
add_lcp_single_node_ha(raft/storage_info_rpc_messages.lcp SLK_SERIALIZE)

add_custom_target(generate_lcp_single_node_ha DEPENDS generate_lcp_common ${generated_lcp_single_node_ha_files})

set(MG_SINGLE_NODE_HA_LIBS stdc++fs Threads::Threads fmt cppitertools
    antlr_opencypher_parser_lib dl glog gflags
    mg-utils mg-io mg-integrations-kafka mg-requests mg-communication mg-comm-rpc
    mg-auth)

if (USE_LTALLOC)
  list(APPEND MG_SINGLE_NODE_HA_LIBS ltalloc)
  # TODO(mferencevic): Enable this when clang is updated on apollo.
  # set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -flto")
endif()

if (READLINE_FOUND)
  list(APPEND MG_SINGLE_NODE_HA_LIBS readline)
endif()

add_library(mg-single-node-ha STATIC ${mg_single_node_ha_sources})
target_link_libraries(mg-single-node-ha ${MG_SINGLE_NODE_HA_LIBS})
add_dependencies(mg-single-node-ha generate_opencypher_parser)
add_dependencies(mg-single-node-ha generate_lcp_single_node_ha)
target_compile_definitions(mg-single-node-ha PUBLIC MG_SINGLE_NODE_HA)

# ----------------------------------------------------------------------------
# END Memgraph Single Node High Availability
# ----------------------------------------------------------------------------

string(TOLOWER ${CMAKE_BUILD_TYPE} lower_build_type)

# STATIC library used to store key-value pairs
add_library(kvstore_lib STATIC storage/common/kvstore/kvstore.cpp)
target_link_libraries(kvstore_lib stdc++fs mg-utils rocksdb bzip2 zlib glog gflags)

# STATIC library for dummy key-value storage
add_library(kvstore_dummy_lib STATIC storage/common/kvstore/kvstore_dummy.cpp)
target_link_libraries(kvstore_dummy_lib mg-utils)

# Generate a version.hpp file
set(VERSION_STRING ${memgraph_VERSION})
configure_file(version.hpp.in version.hpp @ONLY)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# memgraph main executable
add_executable(memgraph memgraph.cpp)
target_link_libraries(memgraph mg-single-node kvstore_lib telemetry_lib)
set_target_properties(memgraph PROPERTIES
                      # Set the executable output name to include version information.
                      OUTPUT_NAME "memgraph-${memgraph_VERSION}-${COMMIT_HASH}_${CMAKE_BUILD_TYPE}"
                      # Output the executable in main binary dir.
                      RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
# Create symlink to the built executable.
add_custom_command(TARGET memgraph POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E create_symlink $<TARGET_FILE:memgraph> ${CMAKE_BINARY_DIR}/memgraph
                   BYPRODUCTS ${CMAKE_BINARY_DIR}/memgraph
                   COMMENT Creating symlink to memgraph executable)

# Strip the executable in release build.
if (lower_build_type STREQUAL "release")
  add_custom_command(TARGET memgraph POST_BUILD
                     COMMAND strip -s $<TARGET_FILE:memgraph>
                     COMMENT Stripping symbols and sections from memgraph)
endif()

# Everything here is under "memgraph" install component.
set(CMAKE_INSTALL_DEFAULT_COMPONENT_NAME "memgraph")

# TODO: Default directory permissions to 755
# NOTE: This is added in CMake 3.11, so enable it then
#set(CMAKE_INSTALL_DEFAULT_DIRECTORY_PERMISSIONS
#    OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ WORLD_READ)

# Install and rename executable to just 'memgraph' Since we have to rename,
# we cannot use the recommended `install(TARGETS ...)`.
install(PROGRAMS $<TARGET_FILE:memgraph>
        DESTINATION lib/memgraph RENAME memgraph)
# Install the config file (must use absolute path).
install(FILES ${CMAKE_SOURCE_DIR}/config/community.conf
        DESTINATION /etc/memgraph RENAME memgraph.conf)
# Install logrotate configuration (must use absolute path).
install(FILES ${CMAKE_SOURCE_DIR}/release/logrotate.conf
        DESTINATION /etc/logrotate.d RENAME memgraph)
install(FILES ${CMAKE_SOURCE_DIR}/release/logrotate_audit.conf
        DESTINATION /etc/logrotate.d RENAME memgraph_audit)
# Create empty directories for default location of lib and log.
install(CODE "file(MAKE_DIRECTORY \$ENV{DESTDIR}/var/log/memgraph
                   \$ENV{DESTDIR}/var/lib/memgraph)")
# Install the license file.
install(FILES ${CMAKE_SOURCE_DIR}/release/LICENSE.md
        DESTINATION share/doc/memgraph RENAME copyright)
# Install systemd service (must use absolute path).
install(FILES ${CMAKE_SOURCE_DIR}/release/memgraph.service
        DESTINATION /lib/systemd/system)

# Install examples
set(examples ${CMAKE_SOURCE_DIR}/release/examples)
install(
  CODE
  "execute_process(COMMAND ${examples}/build_examples
                   ${CMAKE_BINARY_DIR}/memgraph
                   ${CMAKE_BINARY_DIR}/tests/manual/bolt_client
                   WORKING_DIRECTORY ${examples})")
install(DIRECTORY ${examples}/build/ DESTINATION share/memgraph/examples)


# memgraph distributed main executable
add_executable(memgraph_distributed memgraph_distributed.cpp)
target_link_libraries(memgraph_distributed mg-distributed kvstore_lib telemetry_lib)
set_target_properties(memgraph_distributed PROPERTIES
                      # Set the executable output name to include version information.
                      OUTPUT_NAME "memgraph_distributed-${memgraph_VERSION}-${COMMIT_HASH}_${CMAKE_BUILD_TYPE}"
                      # Output the executable in main binary dir.
                      RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
# Create symlink to the built executable.
add_custom_command(TARGET memgraph_distributed POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E create_symlink $<TARGET_FILE:memgraph_distributed> ${CMAKE_BINARY_DIR}/memgraph_distributed
                   BYPRODUCTS ${CMAKE_BINARY_DIR}/memgraph_distributed
                   COMMENT Creating symlink to memgraph distributed executable)

# memgraph single node high availability executable
add_executable(memgraph_ha memgraph_ha.cpp)
target_link_libraries(memgraph_ha mg-single-node-ha kvstore_lib telemetry_lib)
set_target_properties(memgraph_ha PROPERTIES
                      # Set the executable output name to include version information.
                      OUTPUT_NAME "memgraph_ha-${memgraph_VERSION}-${COMMIT_HASH}_${CMAKE_BUILD_TYPE}"
                      # Output the executable in main binary dir.
                      RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
# Create symlink to the built executable.
add_custom_command(TARGET memgraph_ha POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E create_symlink $<TARGET_FILE:memgraph_ha> ${CMAKE_BINARY_DIR}/memgraph_ha
                   BYPRODUCTS ${CMAKE_BINARY_DIR}/memgraph_ha
                   COMMENT Creating symlink to memgraph single node high availability executable)
