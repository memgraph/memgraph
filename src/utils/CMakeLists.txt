set(utils_src_files
    async_timer.cpp
    event_counter.cpp
    csv_parsing.cpp
    file.cpp
    file_locker.cpp
    memory.cpp
    memory_tracker.cpp
    readable_size.cpp
    signals.cpp
    sysinfo/memory.cpp
    thread.cpp
    thread_pool.cpp
    uuid.cpp)

add_library(mg-utils STATIC ${utils_src_files})
target_link_libraries(mg-utils stdc++fs Threads::Threads spdlog fmt gflags uuid rt jemalloc)

add_library(mg-new-delete STATIC new_delete.cpp)
target_link_libraries(mg-new-delete jemalloc fmt)

add_library(mg-purge STATIC purge.cpp)
target_link_libraries(mg-purge jemalloc)
