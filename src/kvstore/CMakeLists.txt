set(kvstore_src_files kvstore.cpp)
if(MG_ENTERPRISE)
    define_add_lcp(add_lcp_kvstore lcp_kvstore_cpp_files generated_lcp_kvstore_files)

    add_lcp_kvstore(replication/rpc.lcp SLK_SERIALIZE)

    add_custom_target(generate_lcp_kvstore DEPENDS ${generated_lcp_kvstore_files})

    set(kvstore_src_files
        ${kvstore_src_files}
        ${lcp_kvstore_cpp_files})
endif()

# STATIC library used to store key-value pairs
add_library(mg-kvstore STATIC ${kvstore_src_files})
target_link_libraries(mg-kvstore stdc++fs mg-utils rocksdb bzip2 zlib glog gflags)

if(MG_ENTERPRISE)
    add_dependencies(mg-kvstore generate_lcp_kvstore)
    target_link_libraries(mg-kvstore mg-rpc mg-slk)
endif()

# STATIC library for dummy key-value storage
# add_library(mg-kvstore-dummy STATIC kvstore_dummy.cpp)
# target_link_libraries(mg-kvstore-dummy mg-utils)
