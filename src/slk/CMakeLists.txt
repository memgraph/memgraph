set(slk_src_files
    streams.cpp
    utils_serialization.cpp)

find_package(fmt REQUIRED)
find_package(gflags REQUIRED)

add_library(mg-slk STATIC ${slk_src_files})
add_library(mg::slk ALIAS mg-slk)
target_link_libraries(mg-slk PUBLIC fmt::fmt mg-utils PRIVATE gflags)
