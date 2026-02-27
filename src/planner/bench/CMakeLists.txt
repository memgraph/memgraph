add_executable(
    memgraph__benchmark__new_planner
)

target_sources(memgraph__benchmark__new_planner
    PRIVATE
    bench_main.cpp
    bench_union_find.cpp
    bench_egraph_rebuild.cpp
    bench_ematch.cpp
    bench_vm.cpp
    bench_join.cpp
    bench_rewrite.cpp
)

target_link_libraries(memgraph__benchmark__new_planner PRIVATE mg::planner benchmark::benchmark)

add_dependencies(memgraph__benchmark memgraph__benchmark__new_planner)

# Egglog comparison benchmark (uses gtest fixture infra)
add_executable(benchmark__egglog benchmark__egglog.cpp)
target_link_libraries(benchmark__egglog PRIVATE mg::planner::test_helpers)
if(TARGET memgraph_unit_main)
    target_link_libraries(benchmark__egglog PRIVATE memgraph_unit_main)
else()
    target_link_libraries(benchmark__egglog PRIVATE gtest_main Threads::Threads dl)
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    find_package(fmt REQUIRED)

    # Standalone benchmark tool for EMatcher vs VM (reads fuzzer corpus files)
    add_executable(bench_ematch_corpus bench_ematch_corpus.cpp)
    target_link_libraries(bench_ematch_corpus PRIVATE mg::planner fmt::fmt)

    # Profiling tool for perf analysis
    add_executable(profile_ematch profile_ematch.cpp)
    target_link_libraries(profile_ematch PRIVATE mg::planner fmt::fmt)
endif()
