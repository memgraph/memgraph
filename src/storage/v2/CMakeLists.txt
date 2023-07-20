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
    replication/rpc.cpp)

#######################
find_package(gflags REQUIRED)
find_package(Threads REQUIRED)

add_library(mg-storage-v2 STATIC ${storage_v2_src_files})
target_link_libraries(mg-storage-v2 Threads::Threads mg-utils gflags)

target_link_libraries(mg-storage-v2 mg-rpc mg-slk)
