add_library(mg-storage-v2 STATIC)
add_library(mg::storage ALIAS mg-storage-v2)

target_sources(mg-storage-v2
        PRIVATE
        commit_log.cpp
        constraints/existence_constraints.cpp
        constraints/constraints.cpp
        constraint_verification_info.cpp
        temporal.cpp
        durability/durability.cpp
        durability/serialization.cpp
        durability/snapshot.cpp
        durability/wal.cpp
        edge_accessor.cpp
        property_store.cpp
        vertex_accessor.cpp
        vertex_info_cache.cpp
        storage.cpp
        indices/indices.cpp
        indices/text_index.cpp
        all_vertices_iterable.cpp
        edges_iterable.cpp
        vertices_iterable.cpp
        inmemory/storage.cpp
        inmemory/edge_type_index.cpp
        inmemory/edge_type_property_index.cpp
        inmemory/label_index.cpp
        inmemory/label_property_index.cpp
        inmemory/unique_constraints.cpp
        disk/durable_metadata.cpp
        disk/edge_import_mode_cache.cpp
        disk/storage.cpp
        disk/rocksdb_storage.cpp
        disk/edge_type_property_index.cpp
        disk/edge_type_index.cpp
        disk/label_index.cpp
        disk/label_property_index.cpp
        disk/unique_constraints.cpp
        storage_mode.cpp
        replication/replication_client.cpp
        replication/serialization.cpp
        replication/slk.cpp
        replication/rpc.cpp
        replication/replication_storage_state.cpp
        inmemory/replication/recovery.cpp
        point_functions.cpp

        PUBLIC
        FILE_SET HEADERS
        BASE_DIRS ../../
        FILES
        enum.hpp
        enum_store.hpp
        vertex_info_cache_fwd.hpp
        vertex_info_cache.hpp
        delta_container.hpp
        point.hpp
        point_functions.hpp
)

find_package(gflags REQUIRED)
find_package(Threads REQUIRED)

target_include_directories(mg-storage-v2 PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(mg-storage-v2
    PUBLIC
    mg::replication
    Threads::Threads
    mg-utils
    mg-flags
    gflags
    absl::flat_hash_map
    mg-rpc
    mg-slk
    mg-events
    mg-memory
    lib::rangev3
    mgcxx_text_search
    tantivy_text_search
    strong_type::strong_type
    rangev3
)
