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

mg_split_debug(mg-module-support INSTALL_DESTINATION lib/memgraph)

find_package(antlr4-runtime REQUIRED)
find_package(cppitertools REQUIRED)
find_package(range-v3 REQUIRED)
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
    virtual_graph.cpp
    interpret/awesome_memgraph_functions.cpp
    interpret/eval.cpp
    interpret/frame.cpp
    interpreter_context.cpp
    interpreter.cpp
    jsonl/reader.cpp
    metadata.cpp
    parse_config_map.cpp
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
    plan_v2/egraph/egraph.cpp
    plan_v2/frontend/ast_converter.cpp
    plan_v2/cost/builtin_estimator.cpp
    plan_v2/frontend/egraph_converter.cpp
    plan_v2/frontend/builder.cpp
    plan_v2/cost/cost_model.cpp
    plan_v2/rewrite/rewrites.cpp
    plan_v2/rewrite/fold.cpp
    plan_v2/egraph/symbol_make_traits.cpp
    plan_v2/resolve/analysis.cpp
    plan/parallel_checker.cpp
    procedure/mg_procedure_impl.cpp
    procedure/callable_alias_mapper.cpp
    procedure/cypher_types.cpp
    procedure/mg_procedure_helpers.cpp
    procedure/mg_procedure_impl.cpp
    procedure/module.cpp
    procedure/py_module.cpp
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
    FILE_SET HEADERS
    FILES
    dependant_symbol_visitor.hpp
    plan_v2/rewrite/rewrites.hpp
    plan_v2/rewrite/fold.hpp
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
    plan/parallel_checker.hpp
    plan/point_distance_condition.hpp
    plan/read_write_type_checker.hpp
    plan/rewrite/enum.hpp
    plan/rewrite/order_by_elimination.hpp
    plan/rewrite/parallel_rewrite.hpp
    plan/used_index_checker.hpp
    plan_v2/frontend/ast_converter.hpp
    plan_v2/egraph/egraph.hpp
    plan_v2/frontend/egraph_converter.hpp
    procedure/cypher_types.hpp
    string_helpers.hpp
    vertex_accessor.hpp

    PRIVATE
    FILE_SET private_headers
    TYPE HEADERS
    FILES
    plan_v2/frontend/builder.hpp
    plan_v2/resolve/analysis.hpp
    plan_v2/resolve/constant_identity.hpp
    plan_v2/resolve/extraction_env.hpp
    plan_v2/resolve/resolver.hpp
    plan_v2/egraph/symbol.hpp
    plan_v2/resolve/variable_index.hpp
)

# CXX_MODULES split into a separate target to avoid CMake 4.x CMP0189
# cycle errors (see src/utils/CMakeLists.txt for details).
add_library(mg-query-cppm STATIC)
add_library(mg::query-cppm ALIAS mg-query-cppm)
target_sources(mg-query-cppm
    PUBLIC
    FILE_SET CXX_MODULES FILES
    arrow_parquet/reader.cppm
    jsonl/reader.cppm
)
target_link_libraries(mg-query-cppm PUBLIC mg-aws mg::storage)

if(NOT "${MG_PYTHON_PATH}" STREQUAL "")
    set(Python3_ROOT_DIR "${MG_PYTHON_PATH}")
elseif(NOT DEFINED Python3_ROOT_DIR)
    # The toolchain sysroot ships a Python install solely for GDB --with-python.
    # Without a hint, FindPython3 picks it up first (sysroot leads
    # CMAKE_FIND_ROOT_PATH) and the resulting binary's NEEDED libpython is the
    # toolchain's, not the host's — cmake even warns about the runtime shadow.
    # Force the host Python (matches what the deploy distro will ship).
    set(Python3_ROOT_DIR "/usr")
endif()
set(Python3_FIND_STRATEGY LOCATION)

# FindPython3 hints go through find_path/find_library, which re-root absolute
# paths against CMAKE_FIND_ROOT_PATH — so Python3_ROOT_DIR=/usr gets re-rooted
# to ${SYSROOT}/usr and the sysroot Python wins anyway. Flip include/library
# find modes to NEVER for the duration of this find_package so /usr stays /usr.
set(_mg_saved_include_mode "${CMAKE_FIND_ROOT_PATH_MODE_INCLUDE}")
set(_mg_saved_library_mode "${CMAKE_FIND_ROOT_PATH_MODE_LIBRARY}")
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY NEVER)

if(NOT "${MG_PYTHON_PATH}" STREQUAL "")
    set(Python3_ROOT_DIR "${MG_PYTHON_PATH}")
endif()

# Build against the Python stable ABI (abi3) so the resulting binary works
# against any libpython3 with version >= MG_PYTHON_STABLE_ABI_FLOOR.
# Compile-time headers only need to exist for *some* compatible Python; the
# runtime libpython is resolved via the version-less SONAME (libpython3.so).
if("${MG_PYTHON_VERSION}" STREQUAL "")
    find_package(Python3 3.10 REQUIRED COMPONENTS Development)
else()
    find_package(Python3 "${MG_PYTHON_VERSION}" EXACT REQUIRED COMPONENTS Development)
endif()
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE "${_mg_saved_include_mode}")
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY "${_mg_saved_library_mode}")


# Hex form of the floor version: 3.10 = 0x030a0000.
# 3.10 is the earliest floor where `Py_TYPE`, `Py_INCREF`, and `Py_DECREF` are
# proper stable-ABI functions instead of macros that touch struct internals.
set(MG_PYTHON_STABLE_ABI_FLOOR "0x030a0000" CACHE STRING
    "Minimum Python version we promise to run on (Py_LIMITED_API value)")

# Always link against the full versioned libpython (`Python3::Python`, e.g.
# `libpython3.12.so`). It defines the complete symbol set, so the link
# always resolves — unlike the bare unversioned `libpython3.so`, which on
# some distros (notably RHEL/CentOS 10) is a stub that does NOT export the
# stable-ABI symbols and breaks the link with `undefined symbol` errors.
#
# Runtime version-portability is achieved separately: a POST_BUILD step
# (see cmake/RewriteDtNeededAbi3.cmake, invoked via mg_apply_python_abi3_rewrite
# in src/CMakeLists.txt) rewrites the binary's DT_NEEDED from the versioned
# SONAME the linker recorded (`libpython3.X.so.1.0`) to the unversioned abi3
# SONAME `libpython3.so`. Combined with `Py_LIMITED_API` below, the binary
# then loads against any libpython3 >= the floor at runtime.
set(MG_PYTHON_LINK_TARGET Python3::Python)

# The host libpython was built against the HOST glibc, which may be newer than
# the sysroot's pinned glibc — its undefined symbols (pthread_create@GLIBC_2.34,
# ...) then can't resolve at link time and lld's --no-allow-shlib-undefined
# rejects the link (observed with Arch's libpython3.14). At runtime this is
# moot: the DT_NEEDED rewrite loads the target machine's own python, which
# always matches that machine's glibc. Relax the check for consumers.
target_link_options(Python3::Python INTERFACE "-Wl,--allow-shlib-undefined")

target_compile_definitions(mg-query PUBLIC
    "Py_LIMITED_API=${MG_PYTHON_STABLE_ABI_FLOOR}")

# Debian/Ubuntu split pyconfig.h between /usr/include/pythonX.Y/pyconfig.h
# (a redirect) and /usr/include/<triple>/pythonX.Y/pyconfig.h (the real one).
# cmake's FindPython3 returns only the first path, and our toolchain sysroot
# blocks the redirect's <triple>/pythonX.Y/pyconfig.h fallback through the
# default /usr/include search. -idirafter appends host /usr/include to the
# tail of the search path, so it's only consulted for headers the sysroot
# doesn't have (i.e. just the multiarch pyconfig redirect target).
if(EXISTS "/usr/include/x86_64-linux-gnu/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/pyconfig.h"
    OR EXISTS "/usr/include/aarch64-linux-gnu/python${Python3_VERSION_MAJOR}.${Python3_VERSION_MINOR}/pyconfig.h")
    target_compile_options(Python3::Python INTERFACE -idirafter /usr/include)
endif()

target_link_libraries(mg-query
    PUBLIC
    dl
    cppitertools::cppitertools
    range-v3::range-v3
    ${MG_PYTHON_LINK_TARGET}
    mg-integrations-pulsar
    mg-integrations-kafka
    mg::storage
    mg-license
    mg-utils
    mg-aws
    mg-query-cppm
    mg-events
    mg-kvstore
    mg-memory-utils
    mg::csv
    mg::system
    mg-flags
    mg-dbms
    mg-coordination
    mg-parameters
    mg::planner
    PRIVATE
    arrow::arrow
    mg-module-support
    mg-cpp-api
    ctre::ctre
    simdjson::simdjson
)

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
    ${ANTLR4_EXECUTABLE} -Dlanguage=Cpp -visitor -package antlropencypher -o ${opencypher_generated} ${opencypher_lexer_grammar} ${opencypher_parser_grammar}
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
    DEPENDS
    ${opencypher_lexer_grammar} ${opencypher_parser_grammar}
    ${opencypher_frontend}/grammar/CypherLexer.g4
    ${opencypher_frontend}/grammar/Cypher.g4
    CODEGEN
    VERBATIM)

add_custom_target(generate_opencypher_parser
    DEPENDS ${antlr_opencypher_generated_src} ${antlr_opencypher_generated_include})

add_library(antlr_opencypher_parser_lib STATIC ${antlr_opencypher_generated_src})
add_dependencies(antlr_opencypher_parser_lib generate_opencypher_parser)
target_include_directories(antlr_opencypher_parser_lib PUBLIC ${antlr4-runtime_INCLUDE_DIR})
target_link_libraries(antlr_opencypher_parser_lib PRIVATE antlr4_static)

target_link_libraries(mg-query PUBLIC antlr_opencypher_parser_lib)

if(MG_ENABLE_TESTING)
  add_subdirectory(plan_v2/test_support)
endif()
