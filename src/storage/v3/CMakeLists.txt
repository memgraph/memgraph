define_add_lcp(add_storage_ast lcp_storage_v3_cpp_files generated_lcp_storage_v3_files)

add_storage_ast(bindings/ast/ast.lcp)

add_custom_target(generate_lcp_ast_storage_v3 DEPENDS ${generated_lcp_storage_v3_files})

set(storage_v3_src_files
    ${lcp_storage_v3_cpp_files}
    temporal.cpp
    edge_accessor.cpp
    indices.cpp
    key_store.cpp
    lexicographically_ordered_vertex.cpp
    property_store.cpp
    vertex_accessor.cpp
    schemas.cpp
    schema_validator.cpp
    shard.cpp
    storage.cpp
    shard_rsm.cpp
    bindings/typed_value.cpp
    expr.cpp
    request_helper.cpp
    storage.cpp)

# ######################
find_package(gflags REQUIRED)
find_package(Threads REQUIRED)

add_library(mg-storage-v3 STATIC ${storage_v3_src_files})
add_dependencies(mg-storage-v3 generate_lcp_ast_storage_v3)
target_link_libraries(mg-storage-v3 Threads::Threads mg-utils gflags)
target_include_directories(mg-storage-v3 PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/bindings)

add_dependencies(mg-storage-v3 generate_lcp_storage)
target_link_libraries(mg-storage-v3 mg-slk mg-expr mg-io)
