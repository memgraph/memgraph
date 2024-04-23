add_library(mg-flags STATIC
        audit.cpp
        bolt.cpp
        coordination.cpp
        general.cpp
        isolation_level.cpp
        log_level.cpp
        memory_limit.cpp
        run_time_configurable.cpp
        storage_mode.cpp
        query.cpp
        replication.cpp
        experimental.cpp
        experimental.hpp)
target_include_directories(mg-flags PUBLIC include)
target_link_libraries(mg-flags
        PUBLIC spdlog::spdlog mg-settings mg-utils
        PRIVATE lib::rangev3)
