define_add_lcp(add_lcp_query lcp_query_v2_cpp_files generated_lcp_query_v2_files)

add_lcp_query(frontend/ast/ast.lcp)
add_lcp_query(frontend/semantic/symbol.lcp)
add_lcp_query(plan/operator.lcp)
add_lcp_query(plan/operator_distributed.lcp)

add_custom_target(generate_lcp_query_v2 DEPENDS ${generated_lcp_query_v2_files})

set(mg_query_v2_sources
    ${lcp_query_v2_cpp_files}
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
    interpret/multiframe.cpp
    plan/operator.cpp
    plan/operator_distributed.cpp
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
    serialization/property_value.cpp
    stream/streams.cpp
    stream/sources.cpp
    stream/common.cpp
    trigger.cpp
    trigger_context.cpp
    typed_value.cpp)

find_package(Boost REQUIRED)

add_library(mg-query-v2 STATIC ${mg_query_v2_sources})
add_dependencies(mg-query-v2 generate_lcp_query_v2)
target_include_directories(mg-query-v2 PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(mg-query-v2 dl cppitertools Boost::headers)
target_link_libraries(mg-query-v2 mg-integrations-pulsar mg-integrations-kafka mg-storage-v3 mg-license mg-utils mg-kvstore mg-memory)

if(NOT "${MG_PYTHON_PATH}" STREQUAL "")
    set(Python3_ROOT_DIR "${MG_PYTHON_PATH}")
endif()

if("${MG_PYTHON_VERSION}" STREQUAL "")
    find_package(Python3 3.5 REQUIRED COMPONENTS Development)
else()
    find_package(Python3 "${MG_PYTHON_VERSION}" EXACT REQUIRED COMPONENTS Development)
endif()

target_link_libraries(mg-query-v2 Python3::Python)

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

add_custom_target(generate_opencypher_parser_v2
    DEPENDS ${antlr_opencypher_generated_src} ${antlr_opencypher_generated_include})

add_library(antlr_opencypher_parser_lib_v2 STATIC ${antlr_opencypher_generated_src})
add_dependencies(antlr_opencypher_parser_lib_v2 generate_opencypher_parser_v2)
target_link_libraries(antlr_opencypher_parser_lib_v2 antlr4)

target_link_libraries(mg-query-v2 antlr_opencypher_parser_lib_v2)
