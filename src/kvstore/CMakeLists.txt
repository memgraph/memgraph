find_package(gflags REQUIRED)

add_library(mg-kvstore STATIC)
add_library(mg::kvstore ALIAS mg-kvstore)

target_sources(mg-kvstore
    PRIVATE
    kvstore.cpp
    rocksdb_utils.cpp

    PUBLIC
    FILE_SET HEADERS
    BASE_DIRS ../
    FILES
    kvstore.hpp
    rocksdb_utils.hpp
)

target_link_libraries(mg-kvstore stdc++fs mg-utils BZip2::BZip2 ZLIB::ZLIB gflags)
# TODO(gitbuda): Once RocksDB is proparly compiled under toolchain.
# mg_target_link_library_cmake(6 mg-kvstore RocksDB RocksDB::rocksdb rocksdb)
target_link_libraries(mg-kvstore rocksdb)
