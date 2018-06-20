set(utils_src_files
    demangle.cpp
    file.cpp
    signals.cpp
    thread.cpp
    thread/sync.cpp
    uuid.cpp
    watchdog.cpp)

add_library(mg-utils STATIC ${utils_src_files})
target_link_libraries(mg-utils stdc++fs Threads::Threads fmt glog gflags uuid)
