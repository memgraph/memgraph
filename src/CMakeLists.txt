# CMake configuration for the main memgraph library and executable

# add memgraph sub libraries, ordered by dependency
add_subdirectory(lisp)
add_subdirectory(utils)
add_subdirectory(requests)
add_subdirectory(io)
add_subdirectory(kvstore)
add_subdirectory(telemetry)
add_subdirectory(communication)
add_subdirectory(storage/v2)
add_subdirectory(query)
if (MG_ENTERPRISE)
  add_subdirectory(audit)
  add_subdirectory(auth)
  add_subdirectory(slk)
  add_subdirectory(rpc)
endif()

## ----------------------------------------------------------------------------
## Common LCP files
## ----------------------------------------------------------------------------
#
#define_add_lcp(add_lcp_common lcp_common_cpp_files generated_lcp_common_files)
#
#add_lcp_common(query/frontend/ast/ast.lcp)
#add_lcp_common(query/frontend/semantic/symbol.lcp)
#add_lcp_common(query/plan/operator.lcp)
#
#add_custom_target(generate_lcp_common DEPENDS ${generated_lcp_common_files})
#
## ----------------------------------------------------------------------------
## END Common LCP files
## ----------------------------------------------------------------------------
#
## ----------------------------------------------------------------------------
## Memgraph Single Node High Availability
## ----------------------------------------------------------------------------
#set(mg_single_node_ha_sources
#    ${lcp_common_cpp_files}
#    data_structures/concurrent/skiplist_gc.cpp
#    database/single_node_ha/config.cpp
#    database/single_node_ha/graph_db.cpp
#    database/single_node_ha/graph_db_accessor.cpp
#    durability/single_node_ha/state_delta.cpp
#    durability/single_node_ha/paths.cpp
#    durability/single_node_ha/snapshooter.cpp
#    durability/single_node_ha/recovery.cpp
#    glue/communication.cpp
#    raft/coordination.cpp
#    raft/raft_server.cpp
#    raft/storage_info.cpp
#    query/common.cpp
#    query/frontend/ast/cypher_main_visitor.cpp
#    query/frontend/ast/pretty_print.cpp
#    query/frontend/parsing.cpp
#    query/frontend/semantic/required_privileges.cpp
#    query/frontend/semantic/symbol_generator.cpp
#    query/frontend/stripped.cpp
#    query/interpret/awesome_memgraph_functions.cpp
#    query/interpreter.cpp
#    query/plan/operator.cpp
#    query/plan/preprocess.cpp
#    query/plan/pretty_print.cpp
#    query/plan/profile.cpp
#    query/plan/rewrite/index_lookup.cpp
#    query/plan/rule_based_planner.cpp
#    query/plan/variable_start_planner.cpp
#    query/procedure/mg_procedure_impl.cpp
#    query/procedure/module.cpp
#    query/typed_value.cpp
#    storage/common/constraints/record.cpp
#    storage/common/constraints/unique_constraints.cpp
#    storage/common/types/slk.cpp
#    storage/common/types/property_value_store.cpp
#    storage/common/locking/record_lock.cpp
#    storage/single_node_ha/edge_accessor.cpp
#    storage/single_node_ha/record_accessor.cpp
#    storage/single_node_ha/vertex_accessor.cpp
#    transactions/single_node_ha/engine.cpp
#    memgraph_init.cpp
#)
#if (MG_ENTERPRISE)
#  set(mg_single_node_ha_sources
#      ${mg_single_node_ha_sources}
#      glue/auth.cpp)
#endif()
#
#define_add_lcp(add_lcp_single_node_ha mg_single_node_ha_sources generated_lcp_single_node_ha_files)
#
#add_lcp_single_node_ha(durability/single_node_ha/state_delta.lcp)
#add_lcp_single_node_ha(database/single_node_ha/serialization.lcp SLK_SERIALIZE
#        DEPENDS durability/single_node_ha/state_delta.lcp)
#add_lcp_single_node_ha(raft/raft_rpc_messages.lcp SLK_SERIALIZE)
#add_lcp_single_node_ha(raft/log_entry.lcp SLK_SERIALIZE)
#add_lcp_single_node_ha(raft/snapshot_metadata.lcp SLK_SERIALIZE)
#add_lcp_single_node_ha(raft/storage_info_rpc_messages.lcp SLK_SERIALIZE)
#
#add_custom_target(generate_lcp_single_node_ha DEPENDS generate_lcp_common ${generated_lcp_single_node_ha_files})
#
#set(MG_SINGLE_NODE_HA_LIBS stdc++fs Threads::Threads fmt cppitertools
#    antlr_opencypher_parser_lib dl glog gflags
#    mg-utils mg-io mg-requests mg-communication mg-rpc
#    mg-auth mg-audit)
#
#add_library(mg-single-node-ha STATIC ${mg_single_node_ha_sources})
#target_include_directories(mg-single-node-ha PUBLIC ${CMAKE_SOURCE_DIR}/include)
#target_link_libraries(mg-single-node-ha ${MG_SINGLE_NODE_HA_LIBS})
#add_dependencies(mg-single-node-ha generate_opencypher_parser)
#add_dependencies(mg-single-node-ha generate_lcp_single_node_ha)
#target_compile_definitions(mg-single-node-ha PUBLIC MG_SINGLE_NODE_HA)
## TODO: Make these symbols visible once we add support for custom procedure
## modules in HA.
## NOTE: `include/mg_procedure.syms` describes a pattern match for symbols which
## should be dynamically exported, so that `dlopen` can correctly link the
## symbols in custom procedure module libraries.
## target_link_libraries(mg-single-node-ha "-Wl,--dynamic-list=${CMAKE_SOURCE_DIR}/include/mg_procedure.syms")
#
## ----------------------------------------------------------------------------
## END Memgraph Single Node High Availability
## ----------------------------------------------------------------------------
#
#add_custom_target(generate_lcp)
#add_dependencies(generate_lcp generate_lcp_single_node_ha)

string(TOLOWER ${CMAKE_BUILD_TYPE} lower_build_type)

# Generate a version.hpp file
set(VERSION_STRING ${MEMGRAPH_VERSION})
configure_file(version.hpp.in version.hpp @ONLY)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# ----------------------------------------------------------------------------
# Memgraph Single Node v2 Executable
# ----------------------------------------------------------------------------
set(mg_single_node_v2_sources
    glue/communication.cpp
    memgraph_init.cpp
    memgraph.cpp
)
if (MG_ENTERPRISE)
  set(mg_single_node_v2_sources
      ${mg_single_node_v2_sources}
      glue/auth.cpp)
endif()

set(MG_SINGLE_NODE_V2_LIBS stdc++fs Threads::Threads
    telemetry_lib mg-query mg-communication)
if (MG_ENTERPRISE)
  # These are enterprise subsystems
  set(MG_SINGLE_NODE_V2_LIBS ${MG_SINGLE_NODE_V2_LIBS} mg-auth mg-audit)
endif()

# memgraph main executable
add_executable(memgraph ${mg_single_node_v2_sources})
target_include_directories(memgraph PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(memgraph ${MG_SINGLE_NODE_V2_LIBS})
# NOTE: `include/mg_procedure.syms` describes a pattern match for symbols which
# should be dynamically exported, so that `dlopen` can correctly link the
# symbols in custom procedure module libraries.
target_link_libraries(memgraph "-Wl,--dynamic-list=${CMAKE_SOURCE_DIR}/include/mg_procedure.syms")
set_target_properties(memgraph PROPERTIES
                      # Set the executable output name to include version information.
                      OUTPUT_NAME "memgraph-${MEMGRAPH_VERSION}_${CMAKE_BUILD_TYPE}"
                      # Output the executable in main binary dir.
                      RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
# Create symlink to the built executable.
add_custom_command(TARGET memgraph POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E create_symlink $<TARGET_FILE:memgraph> ${CMAKE_BINARY_DIR}/memgraph
                   BYPRODUCTS ${CMAKE_BINARY_DIR}/memgraph
                   COMMENT "Creating symlink to memgraph executable")

# Strip the executable in release build.
if (lower_build_type STREQUAL "release")
  add_custom_command(TARGET memgraph POST_BUILD
                     COMMAND strip -s $<TARGET_FILE:memgraph>
                     COMMENT "Stripping symbols and sections from memgraph")
endif()

# Generate the configuration file.
add_custom_command(TARGET memgraph POST_BUILD
                   COMMAND ${CMAKE_SOURCE_DIR}/config/generate.py
                               ${CMAKE_BINARY_DIR}/memgraph
                               ${CMAKE_BINARY_DIR}/config/memgraph.conf
                   DEPENDS ${CMAKE_SOURCE_DIR}/config/generate.py
                           ${CMAKE_SOURCE_DIR}/config/flags.yaml
                   BYPRODUCTS ${CMAKE_BINARY_DIR}/config/memgraph.conf
                   COMMENT "Generating memgraph configuration file")

# ----------------------------------------------------------------------------
# END Memgraph Single Node v2 Executable
# ----------------------------------------------------------------------------

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
# Install the include file for writing custom procedures.
install(FILES ${CMAKE_SOURCE_DIR}/include/mg_procedure.h
        DESTINATION include/memgraph)
# Install the config file (must use absolute path).
install(FILES ${CMAKE_BINARY_DIR}/config/memgraph.conf
        DESTINATION /etc/memgraph RENAME memgraph.conf)
# Install logrotate configuration (must use absolute path).
install(FILES ${CMAKE_SOURCE_DIR}/release/logrotate.conf
        DESTINATION /etc/logrotate.d RENAME memgraph)
if (MG_ENTERPRISE)
install(FILES ${CMAKE_SOURCE_DIR}/release/logrotate_audit.conf
        DESTINATION /etc/logrotate.d RENAME memgraph_audit)
endif()
# Create empty directories for default location of lib and log.
install(CODE "file(MAKE_DIRECTORY \$ENV{DESTDIR}/var/log/memgraph
                   \$ENV{DESTDIR}/var/lib/memgraph)")


## memgraph single node high availability executable
#add_executable(memgraph_ha memgraph_ha.cpp)
#target_link_libraries(memgraph_ha mg-single-node-ha mg-kvstore telemetry_lib)
#set_target_properties(memgraph_ha PROPERTIES
#                      # Set the executable output name to include version information.
#                      OUTPUT_NAME "memgraph_ha-${MEMGRAPH_VERSION}_${CMAKE_BUILD_TYPE}"
#                      # Output the executable in main binary dir.
#                      RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
## Create symlink to the built executable.
#add_custom_command(TARGET memgraph_ha POST_BUILD
#                   COMMAND ${CMAKE_COMMAND} -E create_symlink $<TARGET_FILE:memgraph_ha> ${CMAKE_BINARY_DIR}/memgraph_ha
#                   BYPRODUCTS ${CMAKE_BINARY_DIR}/memgraph_ha
#                   COMMENT "Creating symlink to memgraph single node high availability executable")
