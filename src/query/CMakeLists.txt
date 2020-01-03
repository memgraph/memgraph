define_add_lcp(add_lcp_query lcp_query_cpp_files generated_lcp_query_files)

add_lcp_query(frontend/ast/ast.lcp)
add_lcp_query(frontend/semantic/symbol.lcp)
add_lcp_query(plan/operator.lcp)

add_custom_target(generate_lcp_query DEPENDS ${generated_lcp_query_files})

set(mg_query_sources
    ${CMAKE_SOURCE_DIR}/src/glue/auth.cpp # Needed for mg-auth
    ${lcp_query_cpp_files}
    common.cpp
    dump.cpp
    frontend/ast/cypher_main_visitor.cpp
    frontend/ast/pretty_print.cpp
    frontend/parsing.cpp
    frontend/semantic/required_privileges.cpp
    frontend/semantic/symbol_generator.cpp
    frontend/stripped.cpp
    interpret/awesome_memgraph_functions.cpp
    interpreter.cpp
    plan/operator.cpp
    plan/preprocess.cpp
    plan/pretty_print.cpp
    plan/profile.cpp
    plan/rewrite/index_lookup.cpp
    plan/rule_based_planner.cpp
    plan/variable_start_planner.cpp
    procedure/mg_procedure_impl.cpp
    procedure/module.cpp
    typed_value.cpp)

add_library(mg-query STATIC ${mg_query_sources})
add_dependencies(mg-query generate_lcp_query)
add_dependencies(mg-query generate_opencypher_parser)
target_compile_definitions(mg-query PUBLIC MG_SINGLE_NODE_V2)
target_include_directories(mg-query PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(mg-query dl cppitertools antlr_opencypher_parser_lib)
target_link_libraries(mg-query mg-storage-v2)
# These are enterprise subsystems
target_link_libraries(mg-query mg-auth kvstore_lib)

# Build another mg-query with kvstore_dummy_lib for mg-auth
add_library(mg-query-with-kvstore-dummy STATIC ${mg_query_sources})
add_dependencies(mg-query-with-kvstore-dummy generate_lcp_query)
add_dependencies(mg-query-with-kvstore-dummy generate_opencypher_parser)
target_compile_definitions(mg-query-with-kvstore-dummy PUBLIC MG_SINGLE_NODE_V2)
target_include_directories(mg-query-with-kvstore-dummy PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(mg-query-with-kvstore-dummy dl cppitertools antlr_opencypher_parser_lib)
target_link_libraries(mg-query-with-kvstore-dummy mg-storage-v2)
target_link_libraries(mg-query-with-kvstore-dummy mg-auth kvstore_dummy_lib)
