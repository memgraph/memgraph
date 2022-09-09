define_add_lcp(add_lcp_query lcp_query_v2_cpp_files generated_lcp_query_v2_files)

add_lcp_query(frontend/ast/ast.lcp)
add_lcp_query(plan/operator.lcp)

add_custom_target(generate_lcp_query_v2 DEPENDS ${generated_lcp_query_v2_files})

set(mg_query_v2_sources
    ${lcp_query_v2_cpp_files}
    common.cpp
    cypher_query_interpreter.cpp
    dump.cpp
    frontend/semantic/required_privileges.cpp
    frontend/stripped.cpp
    interpret/awesome_memgraph_functions.cpp
    interpreter.cpp
    metadata.cpp
    plan/operator.cpp
    plan/preprocess.cpp
    plan/pretty_print.cpp
    plan/profile.cpp
    plan/read_write_type_checker.cpp
    plan/rewrite/index_lookup.cpp
    plan/rule_based_planner.cpp
    plan/variable_start_planner.cpp
    #    procedure/mg_procedure_impl.cpp
    #    procedure/mg_procedure_helpers.cpp
    #    procedure/module.cpp
    #    procedure/py_module.cpp
    serialization/property_value.cpp
    #    stream/streams.cpp
    #    stream/sources.cpp
    #    stream/common.cpp
    #    trigger.cpp
    #    trigger_context.cpp
    bindings/typed_value.cpp
    accessors.cpp)

find_package(Boost REQUIRED)

add_library(mg-query-v2 STATIC ${mg_query_v2_sources})
add_dependencies(mg-query-v2 generate_lcp_query_v2)
target_include_directories(mg-query-v2 PUBLIC ${CMAKE_SOURCE_DIR}/include)
target_include_directories(mg-query-v2 PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/bindings)
target_link_libraries(mg-query-v2 dl cppitertools Boost::headers)
target_link_libraries(mg-query-v2 mg-integrations-pulsar mg-integrations-kafka mg-storage-v3 mg-license mg-utils mg-kvstore mg-memory)
target_link_libraries(mg-query-v2 mg-expr)

if(NOT "${MG_PYTHON_PATH}" STREQUAL "")
    set(Python3_ROOT_DIR "${MG_PYTHON_PATH}")
endif()

if("${MG_PYTHON_VERSION}" STREQUAL "")
    find_package(Python3 3.5 REQUIRED COMPONENTS Development)
else()
    find_package(Python3 "${MG_PYTHON_VERSION}" EXACT REQUIRED COMPONENTS Development)
endif()

target_link_libraries(mg-query-v2 Python3::Python)
