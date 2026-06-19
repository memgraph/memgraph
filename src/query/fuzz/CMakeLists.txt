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

    # Full pipeline fuzzer: parse + plan + execute against an in-memory database
    add_executable(fuzz_cypher_pipeline EXCLUDE_FROM_ALL)
    target_sources(fuzz_cypher_pipeline PRIVATE
        fuzz_cypher_pipeline.cpp
        ${CMAKE_SOURCE_DIR}/src/glue/communication.cpp
    )
    target_link_libraries(fuzz_cypher_pipeline PRIVATE mg-communication mg-query mg-glue)
    target_compile_options(fuzz_cypher_pipeline PRIVATE -fsanitize=fuzzer,address -fno-omit-frame-pointer)
    target_link_options(fuzz_cypher_pipeline PRIVATE -fsanitize=fuzzer,address)

    file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/corpus_cypher_pipeline)

    add_custom_target(run_fuzz_cypher_pipeline
        COMMAND $<TARGET_FILE:fuzz_cypher_pipeline>
            ${CMAKE_CURRENT_BINARY_DIR}/corpus_cypher_pipeline
            ${CMAKE_CURRENT_SOURCE_DIR}/seed_corpus_fraud_detection/queries
        DEPENDS fuzz_cypher_pipeline
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Running Cypher pipeline fuzzer..."
    )

    add_custom_target(clear_corpus_cypher_pipeline
        COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_CURRENT_BINARY_DIR}/corpus_cypher_pipeline/*
        COMMENT "Clearing Cypher pipeline corpus..."
    )
endif()
