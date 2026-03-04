# Fuzzers and their associated tools for the planner
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    find_package(fmt REQUIRED)

    # Fuzzer for e-graph operations
    add_executable(fuzz_egraph fuzz_egraph.cpp)
    target_link_libraries(fuzz_egraph PRIVATE mg::planner fmt::fmt)
    target_compile_options(fuzz_egraph PRIVATE -fsanitize=fuzzer,address -fno-omit-frame-pointer)
    target_link_options(fuzz_egraph PRIVATE -fsanitize=fuzzer,address)
    target_compile_definitions(fuzz_egraph PUBLIC ASSERT_FUZZ)

    # Fuzzer for VM pattern build, compile, and matching (verified against egglog)
    add_executable(fuzz_pattern_vm fuzz_pattern_vm.cpp)
    target_link_libraries(fuzz_pattern_vm PRIVATE mg::planner fmt::fmt)
    target_compile_options(fuzz_pattern_vm PRIVATE -fsanitize=fuzzer,address -fno-omit-frame-pointer)
    target_link_options(fuzz_pattern_vm PRIVATE -fsanitize=fuzzer,address)
    target_compile_definitions(fuzz_pattern_vm PUBLIC ASSERT_FUZZ)

    # Fuzzer for PatternCompiler contract verification (structural invariants)
    add_executable(fuzz_compiler_contracts fuzz_compiler_contracts.cpp)
    target_link_libraries(fuzz_compiler_contracts PRIVATE mg::planner fmt::fmt)
    target_compile_options(fuzz_compiler_contracts PRIVATE -fsanitize=fuzzer,address -fno-omit-frame-pointer)
    target_link_options(fuzz_compiler_contracts PRIVATE -fsanitize=fuzzer,address)
    target_compile_definitions(fuzz_compiler_contracts PUBLIC ASSERT_FUZZ)

    # Create corpus directories for each fuzzer
    file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/corpus_egraph)
    file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/corpus_pattern_vm)
    file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/corpus_compiler_contracts)

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

    add_custom_target(run_fuzz_pattern_vm
        COMMAND $<TARGET_FILE:fuzz_pattern_vm> ${CMAKE_CURRENT_BINARY_DIR}/corpus_pattern_vm
        DEPENDS fuzz_pattern_vm
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Running pattern VM fuzzer..."
    )

    add_custom_target(run_fuzz_compiler_contracts
        COMMAND $<TARGET_FILE:fuzz_compiler_contracts> ${CMAKE_CURRENT_BINARY_DIR}/corpus_compiler_contracts
        DEPENDS fuzz_compiler_contracts
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Running compiler contracts fuzzer..."
    )

    # Clear corpus directories
    add_custom_target(clear_corpus_egraph
        COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_CURRENT_BINARY_DIR}/corpus_egraph/*
        COMMENT "Clearing e-graph corpus..."
    )

    add_custom_target(clear_corpus_pattern_vm
        COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_CURRENT_BINARY_DIR}/corpus_pattern_vm/*
        COMMENT "Clearing pattern VM corpus..."
    )

    add_custom_target(clear_corpus_compiler_contracts
        COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_CURRENT_BINARY_DIR}/corpus_compiler_contracts/*
        COMMENT "Clearing compiler contracts corpus..."
    )

    add_custom_target(clear_corpus_all
        DEPENDS clear_corpus_egraph clear_corpus_pattern_vm clear_corpus_compiler_contracts
        COMMENT "Clearing all corpus directories..."
    )

endif()
