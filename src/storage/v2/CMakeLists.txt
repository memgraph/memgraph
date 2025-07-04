add_library(mg-storage-v2 STATIC)
add_library(mg::storage ALIAS mg-storage-v2)

target_sources(mg-storage-v2
        PRIVATE
        all_vertices_iterable.cpp
        commit_log.cpp
        constraint_verification_info.cpp
        constraints/constraints.cpp
        constraints/existence_constraints.cpp
        constraints/type_constraints.cpp
        constraints/type_constraints_validator.cpp
        disk/durable_metadata.cpp
        disk/edge_import_mode_cache.cpp
        disk/edge_type_index.cpp
        disk/edge_type_property_index.cpp
        disk/edge_property_index.cpp
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
        indices/edge_type_index.cpp
        indices/indices.cpp
        indices/label_index_stats.cpp
        indices/label_property_index.cpp
        indices/label_property_index_stats.cpp
        indices/point_index.cpp
        indices/point_index_change_collector.cpp
        indices/text_index.cpp
        indices/vector_index.cpp
        inmemory/edge_property_index.cpp
        inmemory/edge_type_index.cpp
        inmemory/edge_type_property_index.cpp
        inmemory/label_index.cpp
        inmemory/label_property_index.cpp
        inmemory/replication/recovery.cpp
        inmemory/storage.cpp
        inmemory/unique_constraints.cpp
        point_functions.cpp
        property_store.cpp
        replication/replication_client.cpp
        replication/replication_storage_state.cpp
        replication/rpc.cpp
        replication/serialization.cpp
        replication/slk.cpp
        storage.cpp
        storage_mode.cpp
        temporal.cpp
        vertex_accessor.cpp
        vertex_info_cache.cpp
        vertices_iterable.cpp
        schema_info.cpp
        edge_ref.cpp
        indices/point_iterator.cpp
        property_value_utils.cpp
        config.cpp
        id_types.cpp

        PUBLIC
        FILE_SET HEADERS
        BASE_DIRS ../../
        FILES
        config.hpp
        constraints/type_constraints.hpp
        constraints/type_constraints_kind.hpp
        constraints/type_constraints_validator.hpp
        delta_container.hpp
        disk/storage.hpp
        edge_ref.hpp
        enum.hpp
        enum_store.hpp
        id_types.hpp
        indices/active_indices.hpp
        indices/point_index.hpp
        indices/point_index_change_collector.hpp
        indices/point_index_expensive_header.hpp
        indices/point_iterator.hpp
        indices/vector_index.hpp
        inmemory/indices_mvcc.hpp
        inmemory/storage.hpp
        mvcc.hpp
        point.hpp
        point_functions.hpp
        property_constants.hpp
        property_store.hpp
        property_value.hpp
        property_value_utils.hpp
        schema_info.hpp
        storage.hpp
        transaction.hpp
        transaction_constants.hpp
        vertex_info_cache.hpp
        vertex_info_cache_fwd.hpp
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
    usearch
    strong_type::strong_type
    rangev3
)

add_subdirectory(fuzz EXCLUDE_FROM_ALL)
