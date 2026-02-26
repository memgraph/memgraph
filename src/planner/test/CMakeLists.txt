add_unit_test(planner
    SOURCES
        unittest__union_find.cpp
        unittest__enode.cpp
        unittest__egraph.cpp
        unittest__extractor.cpp
        unittest__pattern.cpp
        unittest__match_arena.cpp
        unittest__ematch.cpp
        unittest__rewrite.cpp
        unittest__join_order.cpp
        unittest__vm_compiler.cpp
        unittest__vm_executor.cpp
        unittest__vm_state.cpp
        unittest__vm_comparison.cpp
        benchmark__egglog.cpp
    LINK_TARGETS mg::planner
)

# Fuzzer for e-graph operations
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    find_package(fmt REQUIRED)
    add_executable(fuzz_egraph fuzz_egraph.cpp)
    target_link_libraries(fuzz_egraph PRIVATE mg::planner fmt::fmt)
    target_compile_options(fuzz_egraph PRIVATE -fsanitize=fuzzer,address -fno-omit-frame-pointer)
    target_link_options(fuzz_egraph PRIVATE -fsanitize=fuzzer,address)
    target_compile_definitions(fuzz_egraph PUBLIC ASSERT_FUZZ)

    # Fuzzer for e-match verification against egglog
    add_executable(fuzz_ematch fuzz_ematch.cpp)
    target_link_libraries(fuzz_ematch PRIVATE mg::planner fmt::fmt)
    target_compile_options(fuzz_ematch PRIVATE -fsanitize=fuzzer,address -fno-omit-frame-pointer)
    target_link_options(fuzz_ematch PRIVATE -fsanitize=fuzzer,address)
    target_compile_definitions(fuzz_ematch PUBLIC ASSERT_FUZZ)

    # Optional: Create a corpus directory for the fuzzer
    file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/fuzz_corpus)

    # Benchmark tool for EMatcher vs VM performance
    add_executable(bench_ematch bench_ematch.cpp)
    target_link_libraries(bench_ematch PRIVATE mg::planner fmt::fmt)

    # Profiling tool for perf analysis
    add_executable(profile_ematch profile_ematch.cpp)
    target_link_libraries(profile_ematch PRIVATE mg::planner fmt::fmt)
endif()
