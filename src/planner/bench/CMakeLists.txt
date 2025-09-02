add_executable(
    planner_benchmarks
)

target_sources(planner_benchmarks
    PRIVATE
    benchmark_union_find.cpp
)

target_link_libraries(planner_benchmarks PRIVATE mg::planner benchmark)

set_target_properties(
    planner_benchmarks
    PROPERTIES CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF)

# Include directories
target_include_directories(planner_benchmarks PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Add custom target to run benchmarks
add_custom_target(
    run_benchmarks
    COMMAND planner_benchmarks
    DEPENDS planner_benchmarks
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running all benchmarks")

# Add custom target to run benchmarks with specific filters
add_custom_target(
    run_benchmarks_union_find
    COMMAND planner_benchmarks --benchmark_filter=.*union_find.*
    DEPENDS planner_benchmarks
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Running union-find benchmarks")
