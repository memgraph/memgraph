# CMake configuration for the main memgraph library and executable

# all memgraph src files
set(memgraph_src_files
    communication/bolt/v1/decoder/decoded_value.cpp
    communication/bolt/v1/session.cpp
    communication/messaging/distributed.cpp
    communication/messaging/local.cpp
    communication/messaging/protocol.cpp
    communication/rpc/rpc.cpp
    data_structures/concurrent/skiplist_gc.cpp
    database/config.cpp
    database/counters.cpp
    database/graph_db.cpp
    database/graph_db_accessor.cpp
    database/state_delta.cpp
    distributed/coordination_master.cpp
    distributed/coordination_worker.cpp
    durability/paths.cpp
    durability/recovery.cpp
    durability/snapshooter.cpp
    durability/wal.cpp
    io/network/addrinfo.cpp
    io/network/endpoint.cpp
    io/network/socket.cpp
    query/common.cpp
    query/console.cpp
    query/frontend/ast/ast.cpp
    query/frontend/ast/cypher_main_visitor.cpp
    query/frontend/semantic/symbol_generator.cpp
    query/frontend/stripped.cpp
    query/interpret/awesome_memgraph_functions.cpp
    query/interpreter.cpp
    query/plan/operator.cpp
    query/plan/preprocess.cpp
    query/plan/rule_based_planner.cpp
    query/plan/variable_start_planner.cpp
    query/typed_value.cpp
    storage/concurrent_id_mapper_master.cpp
    storage/concurrent_id_mapper_worker.cpp
    storage/edge_accessor.cpp
    storage/locking/record_lock.cpp
    storage/property_value.cpp
    storage/record_accessor.cpp
    storage/vertex_accessor.cpp
    threading/thread.cpp
    transactions/engine_master.cpp
    transactions/engine_single_node.cpp
    transactions/engine_worker.cpp
    utils/file.cpp
    utils/network.cpp
    utils/watchdog.cpp
)
# -----------------------------------------------------------------------------

string(TOLOWER ${CMAKE_BUILD_TYPE} lower_build_type)

# memgraph_lib depend on these libraries
set(MEMGRAPH_ALL_LIBS stdc++fs Threads::Threads fmt cppitertools
    antlr_opencypher_parser_lib dl glog gflags
    ${Boost_IOSTREAMS_LIBRARY_RELEASE}
    ${Boost_SERIALIZATION_LIBRARY_RELEASE})

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

# Generate a version.hpp file
set(VERSION_STRING ${memgraph_VERSION})
configure_file(version.hpp.in version.hpp @ONLY)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# memgraph main executable
add_executable(memgraph memgraph_bolt.cpp)
target_link_libraries(memgraph memgraph_lib)
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
install(CODE "execute_process(COMMAND ${examples}/build_examples
                              WORKING_DIRECTORY ${examples})")
install(DIRECTORY ${examples}/build/ DESTINATION share/memgraph/examples)
