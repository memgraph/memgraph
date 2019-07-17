# CMake configuration for the main memgraph library and executable

# add memgraph sub libraries, ordered by dependency
add_subdirectory(lisp)
add_subdirectory(utils)
add_subdirectory(requests)
add_subdirectory(io)
add_subdirectory(telemetry)
add_subdirectory(communication)

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
    data_structures/concurrent/skiplist_gc.cpp
    database/single_node/config.cpp
    database/single_node/graph_db.cpp
    database/single_node/graph_db_accessor.cpp
    durability/single_node/state_delta.cpp
    durability/single_node/paths.cpp
    durability/single_node/recovery.cpp
    durability/single_node/snapshooter.cpp
    durability/single_node/wal.cpp
    glue/communication.cpp
    query/common.cpp
    query/frontend/ast/cypher_main_visitor.cpp
    query/frontend/ast/pretty_print.cpp
    query/frontend/parsing.cpp
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
    storage/common/constraints/record.cpp
    storage/common/constraints/unique_constraints.cpp
    storage/common/locking/record_lock.cpp
    storage/common/types/property_value.cpp
    storage/common/types/property_value_store.cpp
    storage/single_node/edge_accessor.cpp
    storage/single_node/record_accessor.cpp
    storage/single_node/vertex_accessor.cpp
    transactions/single_node/engine.cpp
    memgraph_init.cpp
)

define_add_lcp(add_lcp_single_node mg_single_node_sources generated_lcp_single_node_files)

add_lcp_single_node(durability/single_node/state_delta.lcp)

add_custom_target(generate_lcp_single_node DEPENDS generate_lcp_common ${generated_lcp_single_node_files})

set(MG_SINGLE_NODE_LIBS stdc++fs Threads::Threads fmt cppitertools
    antlr_opencypher_parser_lib dl glog gflags
    mg-utils mg-io mg-requests mg-communication)

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

add_custom_target(generate_lcp)
add_dependencies(generate_lcp generate_lcp_single_node)

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
