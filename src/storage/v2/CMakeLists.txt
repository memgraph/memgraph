set(storage_v2_src_files
    edge_accessor.cpp
    vertex_accessor.cpp
    storage.cpp)

add_library(mg-storage-v2 STATIC ${storage_v2_src_files})
target_link_libraries(mg-storage-v2 Threads::Threads mg-utils glog gflags)
