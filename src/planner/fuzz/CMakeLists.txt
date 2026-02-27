# Fuzzers and their associated tools for the planner
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    find_package(fmt REQUIRED)

    # Fuzzer for e-graph operations
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

    # Corpus analysis: benchmark EMatcher vs VM on corpus files
    add_executable(corpus_benchmark corpus_benchmark.cpp)
    target_link_libraries(corpus_benchmark PRIVATE mg::planner fmt::fmt)

    # Corpus profiling: run single corpus file for perf analysis
    add_executable(corpus_profile corpus_profile.cpp)
    target_link_libraries(corpus_profile PRIVATE mg::planner fmt::fmt)
endif()
