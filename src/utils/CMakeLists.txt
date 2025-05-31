add_library(mg-utils STATIC)
add_library(mg::utils ALIAS mg-utils)

target_sources(mg-utils
    PRIVATE
    async_timer.cpp
    base64.cpp
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
    system_info.cpp
    uuid.cpp
    build_info.cpp
    query_memory_tracker.cpp
    exponential_backoff.cpp
    compressor.cpp
    logging.cpp
    string.cpp
    scheduler.cpp
    skip_list.cpp
    priority_thread_pool.cpp

    PUBLIC
    FILE_SET HEADERS
    BASE_DIRS ../../
    FILES
    allocator/page_aligned.hpp
    allocator/page_slab_memory_resource.hpp
    exponential_backoff.hpp
    memory_layout.hpp
    small_vector.hpp
    static_vector.hpp
    logging.hpp
    string.hpp
    scheduler.hpp
    priority_thread_pool.hpp
    barrier.hpp
    transparent_compare.hpp
)

find_package(Boost REQUIRED CONFIG)
find_package(fmt REQUIRED)
find_package(gflags REQUIRED)
find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)

target_link_libraries(mg-utils PUBLIC Boost::headers fmt::fmt spdlog::spdlog nlohmann_json::nlohmann_json ZLIB::ZLIB croncpp::croncpp)
target_link_libraries(mg-utils PRIVATE librdtsc stdc++fs Threads::Threads gflags uuid rt mg-flags)

add_library(mg-settings STATIC)
target_sources(mg-settings
    PRIVATE
    settings.cpp
)
target_link_libraries(mg-settings mg-kvstore mg-slk mg-utils)

add_library(mg-events STATIC)
target_sources(mg-events
    PRIVATE
    event_counter.cpp
    event_gauge.cpp
    event_histogram.cpp
    event_trigger.cpp
    event_map.cpp
)
target_link_libraries(mg-events mg-utils nlohmann_json::nlohmann_json)
