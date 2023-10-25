add_library(mg-flags STATIC audit.cpp
                            bolt.cpp
                            general.cpp
                            isolation_level.cpp
                            log_level.cpp
                            memory_limit.cpp
                            run_time_configurable.cpp
                            storage_mode.cpp
                            query.cpp)
target_include_directories(mg-flags PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(mg-flags PUBLIC spdlog::spdlog mg-settings mg-utils)
