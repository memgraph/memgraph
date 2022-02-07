set(utils_src_files
    async_timer.cpp
    base64.cpp
    event_counter.cpp
    csv_parsing.cpp
    file.cpp
    file_locker.cpp
    memory.cpp
    memory_tracker.cpp
    readable_size.cpp
    signals.cpp
    sysinfo/memory.cpp
    temporal.cpp
    thread.cpp
    thread_pool.cpp
    tsc.cpp
    uuid.cpp)

find_package(Boost REQUIRED)
find_package(fmt REQUIRED)
find_package(gflags REQUIRED)
find_package(Threads REQUIRED)

add_library(mg-utils STATIC ${utils_src_files})
target_link_libraries(mg-utils PUBLIC Boost::headers fmt::fmt spdlog::spdlog)
target_link_libraries(mg-utils PRIVATE librdtsc stdc++fs Threads::Threads gflags uuid rt)

set(settings_src_files
    settings.cpp)

add_library(mg-settings STATIC ${settings_src_files})
target_link_libraries(mg-settings mg-kvstore mg-slk mg-utils)

set(license_src_files
    license.cpp)
add_library(mg-license STATIC ${license_src_files})
target_link_libraries(mg-license mg-settings mg-utils)

