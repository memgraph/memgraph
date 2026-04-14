# C++20 module interfaces split into separate targets to avoid CMake 4.x
# CMP0189 cycle errors — the synthetic INTERFACE_LIBRARY that CMake
# creates for FILE_SET CXX_MODULES cannot participate in the
# static-library dependency cycle around mg-utils.
#
# mg-fnv: self-contained, no internal deps — sits below mg-utils
# mg-aws: depends on mg-utils + AWS SDK — sits above mg-utils
add_library(mg-fnv STATIC)
add_library(mg::fnv ALIAS mg-fnv)
target_sources(mg-fnv
    PUBLIC
    FILE_SET CXX_MODULES FILES
    fnv.cppm
)

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
    resource_monitoring.cpp
    safe_string.cpp
    disk_utils.cpp
    shared_quota.cpp

    PUBLIC
    FILE_SET HEADERS
    BASE_DIRS ../../
    FILES
    allocator/page_aligned.hpp
    allocator/page_slab_memory_resource.hpp
    data_queue.hpp
    exponential_backoff.hpp
    fixed_string.hpp
    memory_layout.hpp
    pointer_pack.hpp
    small_vector.hpp
    static_vector.hpp
    logging.hpp
    string.hpp
    scheduler.hpp
    priority_thread_pool.hpp
    barrier.hpp
    transparent_compare.hpp
    compile_time.hpp
    resource_monitoring.hpp
    safe_string.hpp
    shared_quota.hpp
)

find_package(Boost REQUIRED CONFIG)
find_package(fmt REQUIRED)
find_package(gflags REQUIRED)
find_package(Threads REQUIRED)
find_package(spdlog REQUIRED)
find_package(croncpp REQUIRED)

target_link_libraries(mg-utils PUBLIC Boost::headers fmt::fmt spdlog::spdlog nlohmann_json::nlohmann_json ZLIB::ZLIB croncpp::croncpp rollbear::strong_type gflags mg-fnv)
target_link_libraries(mg-utils PRIVATE AWS::aws-sdk-cpp-core AWS::aws-sdk-cpp-s3 AWS::aws-sdk-cpp-transfer librdtsc::librdtsc stdc++fs Threads::Threads uuid rt range-v3::range-v3 mg-slk)

add_library(mg-aws STATIC)
add_library(mg::aws ALIAS mg-aws)
target_sources(mg-aws
    PUBLIC
    FILE_SET CXX_MODULES FILES
    aws.cppm
)
target_link_libraries(mg-aws PUBLIC mg-utils AWS::aws-sdk-cpp-core AWS::aws-sdk-cpp-s3 AWS::aws-sdk-cpp-transfer)

add_library(mg-settings STATIC)
target_sources(mg-settings
    PRIVATE
    settings.cpp
)
target_link_libraries(mg-settings PUBLIC mg-kvstore mg-utils PRIVATE mg-slk)

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
