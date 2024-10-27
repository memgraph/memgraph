find_package(gflags REQUIRED)
find_package(BZip2 REQUIRED)
find_package(ZLIB REQUIRED)

add_library(mg-kvstore STATIC kvstore.cpp)
add_library(mg::kvstore ALIAS mg-kvstore)

target_link_libraries(mg-kvstore stdc++fs mg-utils rocksdb BZip2::BZip2 ZLIB::ZLIB gflags)
mg_link_library(6 mg-kvstore RocksDB RocksDB::rocksdb rocksdb)
