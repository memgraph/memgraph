add_library(mg-storage-v2 STATIC)
add_library(mg::storage ALIAS mg-storage-v2)

target_sources(mg-storage-v2
        PRIVATE
        all_vertices_iterable.cpp
        commit_log.cpp
        constraint_verification_info.cpp
        constraints/constraints.cpp
        constraints/existence_constraints.cpp
        disk/durable_metadata.cpp
        disk/edge_import_mode_cache.cpp
        disk/edge_type_index.cpp
        disk/label_index.cpp
        disk/label_property_index.cpp
        disk/rocksdb_storage.cpp
        disk/storage.cpp
        disk/unique_constraints.cpp
        durability/durability.cpp
        durability/serialization.cpp
        durability/snapshot.cpp
        durability/wal.cpp
        edge_accessor.cpp
        edges_iterable.cpp
        indices/indices.cpp
        indices/text_index.cpp
        inmemory/edge_type_index.cpp
        inmemory/label_index.cpp
        inmemory/label_property_index.cpp
        inmemory/replication/recovery.cpp
        inmemory/storage.cpp
        inmemory/unique_constraints.cpp
        property_store.cpp
        storage.cpp
        storage_mode.cpp
        storage_replication/replication_client.cpp
        storage_replication/replication_storage_state.cpp
        storage_replication/rpc.cpp
        storage_replication/serialization.cpp
        storage_replication/slk.cpp
        temporal.cpp
        vertex_accessor.cpp
        vertex_info_cache.cpp
        vertices_iterable.cpp

        PUBLIC
        FILE_SET HEADERS
        BASE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}
        FILES
        delta_container.hpp
        storage_replication/replication_client.hpp
        vertex_info_cache.hpp
        vertex_info_cache_fwd.hpp

)

find_package(gflags REQUIRED)
find_package(Threads REQUIRED)

target_include_directories(mg-storage-v2 PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(mg-storage-v2 mg::replication Threads::Threads mg-utils mg-flags gflags absl::flat_hash_map mg-rpc mg-slk mg-events mg-memory mgcxx_text_search tantivy_text_search)
