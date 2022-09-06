define_add_lcp(add_storage_ast lcp_storage_v3_cpp_files generated_lcp_storage_v3_files)

add_storage_ast(bindings/ast/ast.lcp)

add_custom_target(generate_lcp_ast_storage_v3 DEPENDS ${generated_lcp_storage_v3_files})

set(storage_v3_src_files
    ${lcp_storage_v3_cpp_files}
    commit_log.cpp
    constraints.cpp
    temporal.cpp
    durability/durability.cpp
    durability/serialization.cpp
    durability/snapshot.cpp
    durability/wal.cpp
    edge_accessor.cpp
    indices.cpp
    key_store.cpp
    lexicographically_ordered_vertex.cpp
    property_store.cpp
    vertex_accessor.cpp
    schemas.cpp
    schema_validator.cpp
    shard.cpp
    bindings/typed_value.cpp
    storage.cpp)

# #### Replication #####
define_add_lcp(add_lcp_storage lcp_storage_cpp_files generated_lcp_storage_files)

add_lcp_storage(replication/rpc.lcp SLK_SERIALIZE)

add_custom_target(generate_lcp_storage_v3 DEPENDS ${generated_lcp_storage_files})

set(storage_v3_src_files
    ${storage_v3_src_files}
    replication/replication_client.cpp
    replication/replication_server.cpp
    replication/serialization.cpp
    replication/slk.cpp
    ${lcp_storage_cpp_files})

# ######################
find_package(gflags REQUIRED)
find_package(Threads REQUIRED)

add_library(mg-storage-v3 STATIC ${storage_v3_src_files})
add_dependencies(mg-storage-v3 generate_lcp_ast_storage_v3)
target_link_libraries(mg-storage-v3 Threads::Threads mg-utils gflags)
target_include_directories(mg-storage-v3 PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/bindings)

add_dependencies(mg-storage-v3 generate_lcp_storage)
target_link_libraries(mg-storage-v3 mg-rpc mg-slk mg-expr)
