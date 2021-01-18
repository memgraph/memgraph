set(utils_src_files
    file.cpp
    file_locker.cpp
    memory.cpp
    signals.cpp
    thread.cpp
    thread_pool.cpp
    uuid.cpp)

add_library(mg-utils STATIC ${utils_src_files})
target_link_libraries(mg-utils stdc++fs Threads::Threads spdlog fmt gflags uuid)
