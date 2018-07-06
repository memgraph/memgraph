# CMake configuration for the main memgraph library and executable

# add memgraph sub libraries, ordered by dependency
add_subdirectory(utils)
add_subdirectory(requests)
add_subdirectory(integrations)
add_subdirectory(io)
add_subdirectory(telemetry)
add_subdirectory(communication)

# all memgraph src files
set(memgraph_src_files
    data_structures/concurrent/skiplist_gc.cpp
    database/config.cpp
    database/counters.cpp
    database/graph_db.cpp
    database/graph_db_accessor.cpp
    database/state_delta.cpp
    distributed/bfs_rpc_clients.cpp
    distributed/bfs_subcursor.cpp
    distributed/cache.cpp
    distributed/cluster_discovery_master.cpp
    distributed/cluster_discovery_worker.cpp
    distributed/coordination.cpp
    distributed/coordination_master.cpp
    distributed/coordination_worker.cpp
    distributed/data_manager.cpp
    distributed/data_rpc_clients.cpp
    distributed/data_rpc_server.cpp
    distributed/durability_rpc_clients.cpp
    distributed/durability_rpc_server.cpp
    distributed/index_rpc_server.cpp
    distributed/plan_consumer.cpp
    distributed/plan_dispatcher.cpp
    distributed/produce_rpc_server.cpp
    distributed/pull_rpc_clients.cpp
    distributed/serialization.cpp
    distributed/updates_rpc_clients.cpp
    distributed/updates_rpc_server.cpp
    durability/paths.cpp
    durability/recovery.cpp
    durability/snapshooter.cpp
    durability/wal.cpp
    glue/conversion.cpp
    query/common.cpp
    query/frontend/ast/ast.cpp
    query/frontend/ast/cypher_main_visitor.cpp
    query/frontend/semantic/symbol_generator.cpp
    query/frontend/stripped.cpp
    query/interpret/awesome_memgraph_functions.cpp
    query/interpreter.cpp
    query/plan/distributed.cpp
    query/plan/operator.cpp
    query/plan/preprocess.cpp
    query/plan/rule_based_planner.cpp
    query/plan/variable_start_planner.cpp
    query/repl.cpp
    query/typed_value.cpp
    stats/metrics.cpp
    stats/stats.cpp
    storage/concurrent_id_mapper_master.cpp
    storage/concurrent_id_mapper_worker.cpp
    storage/dynamic_graph_partitioner/dgp.cpp
    storage/dynamic_graph_partitioner/vertex_migrator.cpp
    storage/edge_accessor.cpp
    storage/locking/record_lock.cpp
    storage/property_value.cpp
    storage/property_value_store.cpp
    storage/record_accessor.cpp
    storage/vertex_accessor.cpp
    transactions/engine_master.cpp
    transactions/engine_single_node.cpp
    transactions/engine_worker.cpp
    transactions/snapshot.cpp
)
# -----------------------------------------------------------------------------

# Use this function to add each capnp file to generation. This way each file is
# standalone and we avoid recompiling everything.
# NOTE: memgraph_src_files and generated_capnp_files are globally updated.
function(add_capnp capnp_src_file)
  set(cpp_file ${CMAKE_CURRENT_SOURCE_DIR}/${capnp_src_file}.c++)
  set(h_file ${CMAKE_CURRENT_SOURCE_DIR}/${capnp_src_file}.h)
  add_custom_command(OUTPUT ${cpp_file} ${h_file}
    COMMAND ${CAPNP_EXE} compile -o${CAPNP_CXX_EXE} ${capnp_src_file} -I ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${capnp_src_file} capnproto-proj
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  # Update *global* generated_capnp_files
  set(generated_capnp_files ${generated_capnp_files} ${cpp_file} ${h_file} PARENT_SCOPE)
  # Update *global* memgraph_src_files
  set(memgraph_src_files ${memgraph_src_files} ${cpp_file} PARENT_SCOPE)
endfunction(add_capnp)

# Lisp C++ Preprocessing

set(lcp_exe ${CMAKE_SOURCE_DIR}/tools/lcp)
set(lcp_src_files lisp/lcp.lisp ${lcp_exe})

# Use this function to add each lcp file to generation. This way each file is
# standalone and we avoid recompiling everything.
#
# You may pass a CAPNP_SCHEMA <id> keyword argument to generate the Cap'n Proto
# serialization code from .lcp file. You still need to add the generated capnp
# file through `add_capnp` function. To generate the <id> use `capnp id`
# invocation, and specify it here. This preserves correct id information across
# multiple schema generations. If this wasn't the case, wrong typeId
# information will break RPC between different compilations of memgraph.
#
# NOTE: memgraph_src_files and generated_lcp_files are globally updated.
function(add_lcp lcp_file)
  set(one_value_kwargs CAPNP_SCHEMA)
  cmake_parse_arguments(KW "" "${one_value_kwargs}" "" ${ARGN})
  string(REGEX REPLACE "\.lcp$" ".hpp" h_file
         "${CMAKE_CURRENT_SOURCE_DIR}/${lcp_file}")
  if (KW_CAPNP_SCHEMA)
    string(REGEX REPLACE "\.lcp$" ".capnp" capnp_file
           "${CMAKE_CURRENT_SOURCE_DIR}/${lcp_file}")
    set(capnp_id ${KW_CAPNP_SCHEMA})
    set(capnp_depend capnproto-proj)
    set(cpp_file ${CMAKE_CURRENT_SOURCE_DIR}/${lcp_file}.cpp)
    # Update *global* memgraph_src_files
    set(memgraph_src_files ${memgraph_src_files} ${cpp_file} PARENT_SCOPE)
  endif()
  add_custom_command(OUTPUT ${h_file} ${cpp_file} ${capnp_file}
    COMMAND ${lcp_exe} ${lcp_file} ${capnp_id}
    VERBATIM
    DEPENDS ${lcp_file} ${lcp_src_files} ${capnp_depend}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  # Update *global* generated_lcp_files
  set(generated_lcp_files ${generated_lcp_files} ${h_file} ${cpp_file} ${capnp_file} PARENT_SCOPE)
endfunction(add_lcp)

add_lcp(database/counters_rpc_messages.lcp CAPNP_SCHEMA @0x95a2c3ea3871e945)
add_capnp(database/counters_rpc_messages.capnp)
add_lcp(database/state_delta.lcp CAPNP_SCHEMA @0xdea01657b3563887)
add_capnp(database/state_delta.capnp)
add_lcp(distributed/bfs_rpc_messages.lcp CAPNP_SCHEMA @0x8e508640b09b6d2a)
add_capnp(distributed/bfs_rpc_messages.capnp)
add_lcp(distributed/coordination_rpc_messages.lcp CAPNP_SCHEMA @0x93df0c4703cf98fb)
add_capnp(distributed/coordination_rpc_messages.capnp)
add_lcp(distributed/data_rpc_messages.lcp CAPNP_SCHEMA @0xc1c8a341ba37aaf5)
add_capnp(distributed/data_rpc_messages.capnp)
add_lcp(distributed/durability_rpc_messages.lcp CAPNP_SCHEMA @0xf5e53bc271e2163d)
add_capnp(distributed/durability_rpc_messages.capnp)
add_lcp(distributed/index_rpc_messages.lcp CAPNP_SCHEMA @0xa8aab46862945bd6)
add_capnp(distributed/index_rpc_messages.capnp)
add_lcp(distributed/plan_rpc_messages.lcp CAPNP_SCHEMA @0xfcbc48dc9f106d28)
add_capnp(distributed/plan_rpc_messages.capnp)
add_lcp(distributed/pull_produce_rpc_messages.lcp CAPNP_SCHEMA @0xa78a9254a73685bd)
add_capnp(distributed/pull_produce_rpc_messages.capnp)
add_lcp(distributed/storage_gc_rpc_messages.lcp CAPNP_SCHEMA @0xd705663dfe36cf81)
add_capnp(distributed/storage_gc_rpc_messages.capnp)
add_lcp(distributed/token_sharing_rpc_messages.lcp CAPNP_SCHEMA @0x8f295db54ec4caec)
add_capnp(distributed/token_sharing_rpc_messages.capnp)
add_lcp(distributed/transactional_cache_cleaner_rpc_messages.lcp CAPNP_SCHEMA @0xe2be6183a1ff9e11)
add_capnp(distributed/transactional_cache_cleaner_rpc_messages.capnp)
add_lcp(distributed/updates_rpc_messages.lcp CAPNP_SCHEMA @0x82d5f38d73c7b53a)
add_capnp(distributed/updates_rpc_messages.capnp)
add_lcp(query/plan/operator.lcp CAPNP_SCHEMA @0xe5cae8d045d30c42)
add_capnp(query/plan/operator.capnp)
add_lcp(stats/stats_rpc_messages.lcp CAPNP_SCHEMA @0xc19a87c81b9b4512)
add_capnp(stats/stats_rpc_messages.capnp)
add_lcp(storage/concurrent_id_mapper_rpc_messages.lcp CAPNP_SCHEMA @0xa6068dae93d225dd)
add_capnp(storage/concurrent_id_mapper_rpc_messages.capnp)
add_lcp(transactions/engine_rpc_messages.lcp CAPNP_SCHEMA @0xde02b7c49180cad5)
add_capnp(transactions/engine_rpc_messages.capnp)

add_custom_target(generate_lcp DEPENDS ${generated_lcp_files})

# Registering capnp must come after registering lcp files.

add_capnp(communication/rpc/messages.capnp)
add_capnp(distributed/serialization.capnp)
add_capnp(durability/recovery.capnp)
add_capnp(query/common.capnp)
add_capnp(query/frontend/ast/ast.capnp)
add_capnp(query/frontend/semantic/symbol.capnp)
add_capnp(storage/serialization.capnp)
add_capnp(transactions/common.capnp)
add_capnp(utils/serialization.capnp)

add_custom_target(generate_capnp DEPENDS generate_lcp ${generated_capnp_files})

# -----------------------------------------------------------------------------

string(TOLOWER ${CMAKE_BUILD_TYPE} lower_build_type)

# memgraph_lib depend on these libraries
set(MEMGRAPH_ALL_LIBS stdc++fs Threads::Threads fmt cppitertools
    antlr_opencypher_parser_lib dl glog gflags capnp kj
    ${Boost_IOSTREAMS_LIBRARY_RELEASE}
    ${Boost_SERIALIZATION_LIBRARY_RELEASE}
    mg-utils mg-io mg-integrations mg-requests mg-communication)

if (USE_LTALLOC)
    list(APPEND MEMGRAPH_ALL_LIBS ltalloc)
# TODO(mferencevic): Enable this when clang is updated on apollo.
#    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -flto")
endif()

if (READLINE_FOUND)
    list(APPEND MEMGRAPH_ALL_LIBS readline)
endif()

# STATIC library used by memgraph executables
add_library(memgraph_lib STATIC ${memgraph_src_files})
target_link_libraries(memgraph_lib ${MEMGRAPH_ALL_LIBS})
add_dependencies(memgraph_lib generate_opencypher_parser)
add_dependencies(memgraph_lib generate_lcp)
add_dependencies(memgraph_lib generate_capnp)

# STATIC library used to store key-value pairs
add_library(kvstore_lib STATIC storage/kvstore.cpp)
target_link_libraries(kvstore_lib stdc++fs mg-utils rocksdb bzip2 zlib glog gflags)

# STATIC library for dummy key-value storage
add_library(kvstore_dummy_lib STATIC storage/kvstore_dummy.cpp)
target_link_libraries(kvstore_dummy_lib mg-utils)

# Generate a version.hpp file
set(VERSION_STRING ${memgraph_VERSION})
configure_file(version.hpp.in version.hpp @ONLY)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# memgraph main executable
add_executable(memgraph memgraph_bolt.cpp)
target_link_libraries(memgraph memgraph_lib kvstore_lib telemetry_lib)
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
