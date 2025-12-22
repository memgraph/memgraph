# cmake target for our C++ API
add_library(mg-cpp-api INTERFACE)
target_sources(mg-cpp-api
    INTERFACE
    FILE_SET HEADERS
    BASE_DIRS ${CMAKE_SOURCE_DIR}/include
    FILES
    ${CMAKE_SOURCE_DIR}/include/mgp.hpp
    ${CMAKE_SOURCE_DIR}/include/_mgp.hpp
    ${CMAKE_SOURCE_DIR}/include/mg_exceptions.hpp
    ${CMAKE_SOURCE_DIR}/include/mg_procedure.h
)

# shared library so that linking symbols for the C++ api works
add_library(mg-module-support SHARED)
target_sources(mg-module-support
    PRIVATE
    procedure/mpg_support.cpp
)
target_link_libraries(mg-module-support PRIVATE mg-cpp-api)
set_target_properties(mg-module-support  PROPERTIES
    OUTPUT_NAME "memgraph_module_support"
)

find_package(antlr4-runtime REQUIRED)
find_package(cppitertools REQUIRED)
find_package(simdjson REQUIRED)

find_program(ANTLR4_EXECUTABLE antlr4 REQUIRED)
if(NOT ANTLR4_EXECUTABLE)
    message(FATAL_ERROR "antlr4 executable not found.")
endif()

add_library(mg-query STATIC)
target_sources(mg-query
    PRIVATE
    auth_query_handler.cpp
    arrow_parquet/reader.cpp
    common.cpp
    context.cpp
    cypher_query_interpreter.cpp
    db_accessor.cpp
    dump.cpp
    edge_accessor.cpp
    frontend/ast/ast.cpp
    frontend/ast/cypher_main_visitor.cpp
    frontend/ast/pretty_print.cpp
    frontend/parsing.cpp
    frontend/semantic/required_privileges.cpp
    frontend/semantic/rw_checker.cpp
    frontend/semantic/symbol_generator.cpp
    frontend/semantic/symbol.cpp
    frontend/stripped.cpp
    graph.cpp
    interpret/awesome_memgraph_functions.cpp
    interpret/eval.cpp
    interpret/frame.cpp
    interpreter_context.cpp
    interpreter.cpp
    jsonl/reader.cpp
    metadata.cpp
    plan/hint_provider.cpp
    plan/operator_type_info.cpp
    plan/operator.cpp
    plan/preprocess.cpp
    plan/pretty_print.cpp
    plan/profile.cpp
    plan/read_write_type_checker.cpp
    plan/rewrite/general.cpp
    plan/rewrite/index_lookup.cpp
    plan/rewrite/range.cpp
    plan/rule_based_planner.cpp
    plan/used_index_checker.cpp
    plan/variable_start_planner.cpp
    procedure/callable_alias_mapper.cpp
    procedure/cypher_types.cpp
    procedure/mg_procedure_helpers.cpp
    procedure/mg_procedure_impl.cpp
    procedure/module.cpp
    procedure/py_module.cpp
    query_logger.cpp
    query_user.cpp
    serialization/property_value.cpp
    stream/common.cpp
    stream/sources.cpp
    stream/streams.cpp
    trigger_context.cpp
    trigger.cpp
    typed_value.cpp
    vertex_accessor.cpp

    PUBLIC
    FILE_SET CXX_MODULES
    FILES
    arrow_parquet/reader.cppm
    jsonl/reader.cppm
    FILE_SET HEADERS
    FILES
    dependant_symbol_visitor.hpp
    edge_accessor.hpp
    frontend/ast/ast.hpp
    frontend/ast/query/aggregation.hpp
    frontend/ast/query/auth_query.hpp
    frontend/ast/query/binary_operator.hpp
    frontend/ast/query/exists.hpp
    frontend/ast/query/expression.hpp
    frontend/ast/query/identifier.hpp
    frontend/ast/query/named_expression.hpp
    frontend/ast/query/pattern_comprehension.hpp
    frontend/ast/query/pattern.hpp
    frontend/ast/query/query.hpp
    interpret/eval.hpp
    interpret/frame.hpp
    path.hpp
    plan/point_distance_condition.hpp
    plan/read_write_type_checker.hpp
    plan/rewrite/enum.hpp
    plan/used_index_checker.hpp
    procedure/cypher_types.hpp
    string_helpers.hpp
    vertex_accessor.hpp
)

target_link_libraries(mg-query
    PUBLIC
    dl
    cppitertools::cppitertools
    Python3::Python
    mg-integrations-pulsar
    mg-integrations-kafka
    mg::storage
    mg-license
    mg-utils
    mg-kvstore
    mg-memory-utils
    mg::csv
    mg::system
    mg-flags
    mg-dbms
    mg-events
    PRIVATE
    arrow::arrow
    mg-module-support
    mg-cpp-api
    simdjson::simdjson
)

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
include_directories(${antlr4-runtime_INCLUDE_DIR})

add_custom_command(
    OUTPUT ${antlr_opencypher_generated_src} ${antlr_opencypher_generated_include}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${opencypher_generated}
    COMMAND
    ${ANTLR4_EXECUTABLE} -Dlanguage=Cpp -visitor -package antlropencypher -o ${opencypher_generated} ${opencypher_lexer_grammar} ${opencypher_parser_grammar}
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
    DEPENDS
    ${opencypher_lexer_grammar} ${opencypher_parser_grammar}
    ${opencypher_frontend}/grammar/CypherLexer.g4
    ${opencypher_frontend}/grammar/Cypher.g4)

add_custom_target(generate_opencypher_parser
    DEPENDS ${antlr_opencypher_generated_src} ${antlr_opencypher_generated_include})

add_library(antlr_opencypher_parser_lib STATIC ${antlr_opencypher_generated_src})
add_dependencies(antlr_opencypher_parser_lib generate_opencypher_parser)
target_link_libraries(antlr_opencypher_parser_lib antlr4_static)

target_link_libraries(mg-query PUBLIC antlr_opencypher_parser_lib)
