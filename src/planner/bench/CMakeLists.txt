add_executable(
    memgraph__benchmark__new_planner
)

target_sources(memgraph__benchmark__new_planner
    PRIVATE
    bench_main.cpp
    bench_union_find.cpp
    bench_egraph_rebuild.cpp
)

target_link_libraries(memgraph__benchmark__new_planner PRIVATE mg::planner benchmark)

add_dependencies(memgraph__benchmark memgraph__benchmark__new_planner)
