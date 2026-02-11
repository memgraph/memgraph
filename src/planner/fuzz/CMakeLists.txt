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

    # Create corpus directories for each fuzzer
    file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/corpus_egraph)
    file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/corpus_ematch)

    # Corpus profiling: run single corpus file for perf analysis
    add_executable(corpus_profile corpus_profile.cpp)
    target_link_libraries(corpus_profile PRIVATE mg::planner fmt::fmt)

    # =========================================================================
    # Custom targets for common workflows
    # =========================================================================

    # Run fuzzers (use: cmake --build build --target run_fuzz_egraph)
    add_custom_target(run_fuzz_egraph
        COMMAND $<TARGET_FILE:fuzz_egraph> ${CMAKE_CURRENT_BINARY_DIR}/corpus_egraph
        DEPENDS fuzz_egraph
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Running e-graph fuzzer..."
    )

    add_custom_target(run_fuzz_ematch
        COMMAND $<TARGET_FILE:fuzz_ematch> ${CMAKE_CURRENT_BINARY_DIR}/corpus_ematch
        DEPENDS fuzz_ematch
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Running e-match fuzzer..."
    )

    # Clear corpus directories
    add_custom_target(clear_corpus_egraph
        COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_CURRENT_BINARY_DIR}/corpus_egraph/*
        COMMENT "Clearing e-graph corpus..."
    )

    add_custom_target(clear_corpus_ematch
        COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_CURRENT_BINARY_DIR}/corpus_ematch/*
        COMMENT "Clearing e-match corpus..."
    )

    add_custom_target(clear_corpus_all
        DEPENDS clear_corpus_egraph clear_corpus_ematch
        COMMENT "Clearing all corpus directories..."
    )

endif()
