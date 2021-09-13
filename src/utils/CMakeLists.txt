set(utils_src_files
    async_timer.cpp
    base64.cpp
    event_counter.cpp
    csv_parsing.cpp
    file.cpp
    file_locker.cpp
    license.cpp
    memory.cpp
    memory_tracker.cpp
    readable_size.cpp
    settings.cpp
    signals.cpp
    sysinfo/memory.cpp
    thread.cpp
    thread_pool.cpp
    uuid.cpp)

add_library(mg-utils STATIC ${utils_src_files})
target_link_libraries(mg-utils mg-kvstore mg-slk stdc++fs Threads::Threads spdlog fmt gflags uuid rt)

add_library(mg-new-delete STATIC new_delete.cpp)
target_link_libraries(mg-new-delete jemalloc fmt)
