set(storage_rocks_src_files
    storage.hpp)

find_package(Threads REQUIRED)
find_package(gflags REQUIRED)
find_package(BZip2 REQUIRED)
find_package(ZLIB REQUIRED)

add_library(mg-storage-rocks STATIC ${storage_rocks_src_files})
target_link_libraries(mg-storage-rocks mg-utils Threads::Threads stdc++fs rocksdb BZip2::BZip2 ZLIB::ZLIB)
