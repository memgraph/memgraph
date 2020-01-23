set(utils_src_files
    file.cpp
    memory.cpp
    signals.cpp
    thread.cpp
    uuid.cpp)

add_library(mg-utils STATIC ${utils_src_files})
target_link_libraries(mg-utils stdc++fs Threads::Threads fmt glog gflags uuid)
