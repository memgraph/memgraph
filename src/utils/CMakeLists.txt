find_package(Boost REQUIRED)
find_package(fmt REQUIRED)
find_package(gflags REQUIRED)
find_package(Threads REQUIRED)

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

        PUBLIC
        FILE_SET HEADERS
        BASE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}
        FILES static_vector.hpp
)


target_link_libraries(mg-utils
        PUBLIC Boost::headers fmt::fmt spdlog::spdlog json
        PRIVATE librdtsc stdc++fs Threads::Threads gflags uuid rt)

add_library(mg-settings STATIC settings.cpp)
target_link_libraries(mg-settings mg-kvstore mg-slk mg-utils)

add_library(mg-events STATIC event_counter.cpp event_gauge.cpp event_histogram.cpp event_trigger.cpp event_map.cpp)
target_link_libraries(mg-events mg-utils json)
