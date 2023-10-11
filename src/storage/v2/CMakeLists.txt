#######################
find_package(gflags REQUIRED)
find_package(Threads REQUIRED)

add_library(mg-storage-v2 STATIC
        commit_log.cpp
        constraints/existence_constraints.cpp
        constraints/constraints.cpp
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
        disk/edge_import_mode_cache.cpp
        disk/storage.cpp
        disk/rocksdb_storage.cpp
        disk/label_index.cpp
        disk/label_property_index.cpp
        disk/unique_constraints.cpp
        storage_mode.cpp
        replication/replication_client.cpp
        replication/replication_server.cpp
        replication/serialization.cpp
        replication/slk.cpp
        replication/rpc.cpp
        replication/replication_storage_state.cpp
        replication/replication_handler.cpp
        inmemory/replication/replication_server.cpp
        inmemory/replication/replication_client.cpp
)
target_link_libraries(mg-storage-v2 mg::replication Threads::Threads mg-utils gflags absl::flat_hash_map mg-rpc mg-slk)

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
