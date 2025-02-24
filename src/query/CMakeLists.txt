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

add_library(mg-query STATIC)
target_sources(mg-query
    PRIVATE
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
    frontend/semantic/rw_checker.cpp
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
    plan/rewrite/general.cpp
    plan/rewrite/range.cpp
    plan/rule_based_planner.cpp
    plan/variable_start_planner.cpp
    procedure/mg_procedure_impl.cpp
    procedure/mg_procedure_helpers.cpp
    procedure/module.cpp
    procedure/py_module.cpp
    procedure/cypher_types.cpp
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
    query_user.cpp
    time_to_live/time_to_live.cpp
    query_logger.cpp
    vertex_accessor.cpp
    context.cpp
    edge_accessor.cpp

    PUBLIC
    FILE_SET HEADERS
    FILES
    plan/rewrite/enum.hpp
    plan/point_distance_condition.hpp
    string_helpers.hpp
    path.hpp
    edge_accessor.hpp
    procedure/cypher_types.hpp
    vertex_accessor.hpp
    frontend/ast/ast.hpp
    frontend/ast/query/aggregation.hpp
    frontend/ast/query/auth_query.hpp
    frontend/ast/query/binary_operator.hpp
    frontend/ast/query/exists.hpp
    frontend/ast/query/expression.hpp
    frontend/ast/query/identifier.hpp
    frontend/ast/query/named_expression.hpp
    frontend/ast/query/pattern.hpp
    frontend/ast/query/pattern_comprehension.hpp
    frontend/ast/query/query.hpp
)


target_link_libraries(mg-query
    PUBLIC
    dl
    cppitertools
    rangev3
#    Python3::Python
    mg-integrations-pulsar
    mg-integrations-kafka
    mg::storage
    mg-license
    mg-utils
    mg-kvstore
    mg-memory
    mg::csv
    mg::system
    mg-flags
    mg-dbms
    mg-events
    PRIVATE
    mg-module-support
    mg-cpp-api
)

if(NOT "${MG_PYTHON_PATH}" STREQUAL "")
    set(Python3_ROOT_DIR "${MG_PYTHON_PATH}")
endif()

mg_create_linkable_if_no_cmake(Python3 STATIC "${MG_TOOLCHAIN_ROOT}/lib/libpython3.12.a" "${MG_TOOLCHAIN_ROOT}/include")
# if("${MG_PYTHON_VERSION}" STREQUAL "")
#     find_package(Python3 3.5 REQUIRED COMPONENTS Development)
# else()
#     find_package(Python3 "${MG_PYTHON_VERSION}" EXACT REQUIRED COMPONENTS Development)
# endif()

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

set(ANTLR_GENERATOR_PATH "${CMAKE_SOURCE_DIR}/libs/antlr-4.13.2-complete.jar")
if(DEFINED ENV{MG_TOOLCHAIN_VERSION})
  if($ENV{MG_TOOLCHAIN_VERSION} GREATER_EQUAL 6)
      set(ANTLR_GENERATOR_PATH "${MG_TOOLCHAIN_ROOT}/bin/antlr-4.13.2-complete.jar")
  endif()
endif()
add_custom_command(
    OUTPUT ${antlr_opencypher_generated_src} ${antlr_opencypher_generated_include}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${opencypher_generated}
    COMMAND
    java -jar ${ANTLR_GENERATOR_PATH}
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
