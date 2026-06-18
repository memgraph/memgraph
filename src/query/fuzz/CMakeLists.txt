if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # Grammar-aware fuzzer: generates queries via bytes-as-decisions, runs Parser + CypherMainVisitor
    add_executable(fuzz_cypher_grammar EXCLUDE_FROM_ALL)
    target_sources(fuzz_cypher_grammar PRIVATE fuzz_cypher_grammar.cpp)
    target_include_directories(fuzz_cypher_grammar PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(fuzz_cypher_grammar PRIVATE mg-query antlr4_static)
    target_compile_options(fuzz_cypher_grammar PRIVATE -fsanitize=fuzzer,address -fno-omit-frame-pointer)
    target_link_options(fuzz_cypher_grammar PRIVATE -fsanitize=fuzzer,address)

    file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/corpus_cypher_grammar)

    add_custom_target(run_fuzz_cypher_grammar
        COMMAND $<TARGET_FILE:fuzz_cypher_grammar> ${CMAKE_CURRENT_BINARY_DIR}/corpus_cypher_grammar
        DEPENDS fuzz_cypher_grammar
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Running Cypher grammar fuzzer..."
    )

    add_custom_target(clear_corpus_cypher_grammar
        COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_CURRENT_BINARY_DIR}/corpus_cypher_grammar/*
        COMMENT "Clearing Cypher grammar corpus..."
    )

    # Raw bytes fuzzer: feeds arbitrary input directly to Parser
    add_executable(fuzz_cypher_parser EXCLUDE_FROM_ALL)
    target_sources(fuzz_cypher_parser PRIVATE fuzz_cypher_parser.cpp)
    target_link_libraries(fuzz_cypher_parser PRIVATE mg-query antlr4_static)
    target_compile_options(fuzz_cypher_parser PRIVATE -fsanitize=fuzzer,address -fno-omit-frame-pointer)
    target_link_options(fuzz_cypher_parser PRIVATE -fsanitize=fuzzer,address)

    file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/corpus_cypher_parser)

    add_custom_target(run_fuzz_cypher_parser
        COMMAND $<TARGET_FILE:fuzz_cypher_parser>
            ${CMAKE_CURRENT_BINARY_DIR}/corpus_cypher_parser
            ${CMAKE_CURRENT_SOURCE_DIR}/seed_corpus_cypher_parser
        DEPENDS fuzz_cypher_parser
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Running Cypher parser fuzzer..."
    )

    add_custom_target(clear_corpus_cypher_parser
        COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_CURRENT_BINARY_DIR}/corpus_cypher_parser/*
        COMMENT "Clearing Cypher parser corpus..."
    )
endif()
