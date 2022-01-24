set(slk_src_files
    streams.cpp)

find_package(gflags REQUIRED)

add_library(mg-slk STATIC ${slk_src_files})
target_link_libraries(mg-slk gflags)
target_link_libraries(mg-slk mg-utils)
