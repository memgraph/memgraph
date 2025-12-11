add_library(mg-flags STATIC)
add_library(mg::flags ALIAS mg-flags)
target_sources(mg-flags
        PUBLIC
        all.hpp
        audit.hpp
        auth.hpp
        bolt.hpp
        coord_flag_env_handler.hpp
        coordination.hpp
        experimental.hpp
        general.hpp
        isolation_level.hpp
        log_level.hpp
        memory_limit.hpp
        query.hpp
        replication.hpp
        run_time_configurable.hpp
        storage_access.hpp
        storage_mode.hpp
        scheduler.hpp

        PRIVATE
        audit.cpp
        auth.cpp
        bolt.cpp
        coord_flag_env_handler.cpp
        coordination.cpp
        experimental.cpp
        general.cpp
        isolation_level.cpp
        log_level.cpp
        memory_limit.cpp
        query.cpp
        replication.cpp
        run_time_configurable.cpp
        storage_access.cpp
        storage_mode.cpp
        scheduler.cpp
)
target_include_directories(mg-flags PUBLIC include)

find_package(spdlog REQUIRED)
find_package(croncpp REQUIRED)

target_link_libraries(mg-flags
        PUBLIC spdlog::spdlog mg-settings mg-utils mg-license gflags
        PRIVATE croncpp::croncpp)
