add_library(mg-metrics STATIC)

target_sources(mg-metrics
    PRIVATE
    prometheus_metrics.cpp
)

target_link_libraries(mg-metrics PUBLIC prometheus-cpp::core mg-utils mg-flags)

target_include_directories(mg-metrics PUBLIC ${CMAKE_SOURCE_DIR}/src)
