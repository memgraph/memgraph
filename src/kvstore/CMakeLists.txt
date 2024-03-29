find_package(gflags REQUIRED)
find_package(BZip2 REQUIRED)
find_package(ZLIB REQUIRED)

# STATIC library used to store key-value pairs
add_library(mg-kvstore STATIC kvstore.cpp)
add_library(mg::kvstore ALIAS mg-kvstore)
target_link_libraries(mg-kvstore stdc++fs mg-utils rocksdb BZip2::BZip2 ZLIB::ZLIB gflags)
