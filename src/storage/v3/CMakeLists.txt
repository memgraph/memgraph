set(storage_v3_src_files
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
    storage.cpp)

# ######################
find_package(gflags REQUIRED)
find_package(Threads REQUIRED)

add_library(mg-storage-v3 STATIC ${storage_v3_src_files})
target_link_libraries(mg-storage-v3 Threads::Threads mg-utils gflags)

add_dependencies(mg-storage-v3 generate_lcp_storage)
target_link_libraries(mg-storage-v3 mg-slk mg-expr mg-io)
