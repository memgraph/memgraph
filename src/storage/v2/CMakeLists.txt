set(storage_v2_src_files
    commit_log.cpp
    commit_error.cpp
    constraints.cpp
    temporal.cpp
    durability/durability.cpp
    durability/serialization.cpp
    durability/snapshot.cpp
    durability/wal.cpp
    edge_accessor.cpp
    indices.cpp
    property_store.cpp
    vertex_accessor.cpp
    storage.cpp)

##### Replication #####
define_add_lcp(add_lcp_storage lcp_storage_cpp_files generated_lcp_storage_files)

add_lcp_storage(replication/rpc.lcp SLK_SERIALIZE)

add_custom_target(generate_lcp_storage DEPENDS ${generated_lcp_storage_files})

set(storage_v2_src_files
    ${storage_v2_src_files}
    replication/replication_client.cpp
    replication/replication_server.cpp
    replication/serialization.cpp
    replication/slk.cpp
    replication/replication_persistance_helper.cpp
    ${lcp_storage_cpp_files})

#######################
find_package(gflags REQUIRED)
find_package(Threads REQUIRED)

add_library(mg-storage-v2 STATIC ${storage_v2_src_files})
target_link_libraries(mg-storage-v2 Threads::Threads mg-utils gflags)

add_dependencies(mg-storage-v2 generate_lcp_storage)
target_link_libraries(mg-storage-v2 mg-rpc mg-slk)
