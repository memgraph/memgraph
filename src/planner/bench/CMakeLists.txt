add_executable(
    memgraph__benchmark__new_planner
)

target_sources(memgraph__benchmark__new_planner
    PRIVATE
    bench_union_find.cpp
)

target_link_libraries(memgraph__benchmark__new_planner PRIVATE mg::planner benchmark::benchmark)

add_dependencies(memgraph__benchmark memgraph__benchmark__new_planner)
