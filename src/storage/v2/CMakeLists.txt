set(storage_v2_src_files
    commit_log.cpp
    constraints.cpp
    temporal.cpp
    durability/durability.cpp
    durability/serialization.cpp
    durability/snapshot.cpp
    durability/wal.cpp
    edge_accessor.cpp
    indices.cpp
    inmemory/edge_accessor.cpp
    inmemory/storage.cpp
    inmemory/vertex_accessor.cpp
    property_store.cpp
    disk/compaction_filter.cpp
    disk/old_storage.hpp
    disk/old_storage.cpp
    disk/new_storage.hpp
    disk/vertex_accessor.cpp
    disk/vertex_accessor.hpp
    disk/edge_accessor.cpp
    disk/edge_accessor.hpp
    storage.cpp
    vertex_accessor.cpp)


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
