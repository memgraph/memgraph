set(utils_src_files
    event_counter.cpp
    file.cpp
    file_locker.cpp
    memory.cpp
    memory_tracker.cpp
    signals.cpp
    sysinfo/memory.cpp
    thread.cpp
    thread_pool.cpp
    uuid.cpp)

add_library(mg-utils STATIC ${utils_src_files})
target_link_libraries(mg-utils stdc++fs Threads::Threads spdlog fmt gflags uuid)

add_library(mg-new-delete STATIC new_delete.cpp)
target_link_libraries(mg-new-delete jemalloc fmt)
