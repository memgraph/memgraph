set(storage_v2_src_files
    commit_log.cpp
    constraints/existence_constraints.cpp
    temporal.cpp
    durability/durability.cpp
    durability/serialization.cpp
    durability/snapshot.cpp
    durability/wal.cpp
    edge_accessor.cpp
    property_store.cpp
    vertex_accessor.cpp
    vertex_info_cache_fwd.hpp
    vertex_info_cache.hpp
    vertex_info_cache.cpp
    storage.cpp
    indices/indices.cpp
    all_vertices_iterable.cpp
    vertices_iterable.cpp
    inmemory/storage.cpp
    inmemory/label_index.cpp
    inmemory/label_property_index.cpp
    inmemory/unique_constraints.cpp
    disk/storage.cpp
    disk/rocksdb_storage.cpp
    disk/label_index.cpp
    disk/label_property_index.cpp
    disk/unique_constraints.cpp
    storage_mode.cpp
    isolation_level.cpp)


set(storage_v2_src_files
    ${storage_v2_src_files}
    replication/replication_client.cpp
    replication/replication_server.cpp
    replication/serialization.cpp
    replication/slk.cpp
    replication/replication_persistence_helper.cpp
    replication/rpc.cpp
    replication/replication.cpp
    inmemory/replication/replication_server.cpp
    inmemory/replication/replication_client.cpp
)

#######################
find_package(gflags REQUIRED)
find_package(Threads REQUIRED)

add_library(mg-storage-v2 STATIC ${storage_v2_src_files})
target_link_libraries(mg-storage-v2 Threads::Threads mg-utils gflags absl::flat_hash_map)

target_link_libraries(mg-storage-v2 mg-rpc mg-slk)

# Until we get LTO there is an advantage to do some unity builds
set_target_properties(mg-storage-v2
        PROPERTIES
        UNITY_BUILD ON
        UNITY_BUILD_MODE GROUP
)
set_source_files_properties(
        vertex_info_cache.cpp vertex_accessor.cpp
        PROPERTIES UNITY_GROUP "ensure inline of vertex_info_cache"
)
