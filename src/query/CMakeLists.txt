set(mg_query_sources
    frontend/ast/ast.cpp
    frontend/semantic/symbol.cpp
    plan/operator_type_info.cpp
    common.cpp
    cypher_query_interpreter.cpp
    dump.cpp
    frontend/ast/cypher_main_visitor.cpp
    frontend/ast/pretty_print.cpp
    frontend/parsing.cpp
    frontend/semantic/required_privileges.cpp
    frontend/semantic/symbol_generator.cpp
    frontend/stripped.cpp
    interpret/awesome_memgraph_functions.cpp
    interpret/eval.cpp
    interpreter.cpp
    metadata.cpp
    plan/hint_provider.cpp
    plan/operator.cpp
    plan/preprocess.cpp
    plan/pretty_print.cpp
    plan/profile.cpp
    plan/read_write_type_checker.cpp
    plan/rewrite/index_lookup.cpp
    plan/rule_based_planner.cpp
    plan/variable_start_planner.cpp
    procedure/mg_procedure_impl.cpp
    procedure/mg_procedure_helpers.cpp
    procedure/module.cpp
    procedure/py_module.cpp
    procedure/callable_alias_mapper.cpp
    serialization/property_value.cpp
    stream/streams.cpp
    stream/sources.cpp
    stream/common.cpp
    trigger.cpp
    trigger_context.cpp
    typed_value.cpp
    graph.cpp
    db_accessor.cpp
    auth_query_handler.cpp
    interpreter_context.cpp
)

add_library(mg-query STATIC ${mg_query_sources})
target_include_directories(mg-query PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(mg-query PUBLIC dl
    cppitertools
    rangev3
    Python3::Python
    mg-integrations-pulsar
    mg-integrations-kafka
    mg-storage-v2
    mg-license
    mg-utils
    mg-kvstore
    mg-memory
    mg::csv
    mg-flags
    mg-dbms
    mg-events)

if(NOT "${MG_PYTHON_PATH}" STREQUAL "")
    set(Python3_ROOT_DIR "${MG_PYTHON_PATH}")
endif()

if("${MG_PYTHON_VERSION}" STREQUAL "")
    find_package(Python3 3.5 REQUIRED COMPONENTS Development)
else()
    find_package(Python3 "${MG_PYTHON_VERSION}" EXACT REQUIRED COMPONENTS Development)
endif()

# Generate Antlr openCypher parser
set(opencypher_frontend ${CMAKE_CURRENT_SOURCE_DIR}/frontend/opencypher)
set(opencypher_generated ${opencypher_frontend}/generated)
set(opencypher_lexer_grammar ${opencypher_frontend}/grammar/MemgraphCypherLexer.g4)
set(opencypher_parser_grammar ${opencypher_frontend}/grammar/MemgraphCypher.g4)

set(antlr_opencypher_generated_src
    ${opencypher_generated}/MemgraphCypherLexer.cpp
    ${opencypher_generated}/MemgraphCypher.cpp
    ${opencypher_generated}/MemgraphCypherBaseVisitor.cpp
    ${opencypher_generated}/MemgraphCypherVisitor.cpp
)
set(antlr_opencypher_generated_include
    ${opencypher_generated}/MemgraphCypherLexer.h
    ${opencypher_generated}/MemgraphCypher.h
    ${opencypher_generated}/MemgraphCypherBaseVisitor.h
    ${opencypher_generated}/MemgraphCypherVisitor.h
)

add_custom_command(
    OUTPUT ${antlr_opencypher_generated_src} ${antlr_opencypher_generated_include}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${opencypher_generated}
    COMMAND
    java -jar ${CMAKE_SOURCE_DIR}/libs/antlr-4.10.1-complete.jar
    -Dlanguage=Cpp -visitor -package antlropencypher
    -o ${opencypher_generated}
    ${opencypher_lexer_grammar} ${opencypher_parser_grammar}
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
    DEPENDS
    ${opencypher_lexer_grammar} ${opencypher_parser_grammar}
    ${opencypher_frontend}/grammar/CypherLexer.g4
    ${opencypher_frontend}/grammar/Cypher.g4)

add_custom_target(generate_opencypher_parser
    DEPENDS ${antlr_opencypher_generated_src} ${antlr_opencypher_generated_include})

add_library(antlr_opencypher_parser_lib STATIC ${antlr_opencypher_generated_src})
add_dependencies(antlr_opencypher_parser_lib generate_opencypher_parser)
target_link_libraries(antlr_opencypher_parser_lib antlr4)

target_link_libraries(mg-query PUBLIC antlr_opencypher_parser_lib)
