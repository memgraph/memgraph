# Memgraph Query Modules CMake configuration
# You should use the top level CMake configuration with -DQUERY_MODULES=ON
# These modules are meant to be shipped with Memgraph installation.

project(memgraph_query_modules)

disallow_in_source_build()

# Everything that is installed here, should be under the "query_modules" component.
set(CMAKE_INSTALL_DEFAULT_COMPONENT_NAME "query_modules")

add_library(example SHARED example.c)
target_include_directories(example PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_compile_options(example PRIVATE -Wall)

# Strip the library in release build.
string(TOLOWER ${CMAKE_BUILD_TYPE} lower_build_type)
if (lower_build_type STREQUAL "release")
  add_custom_command(TARGET example POST_BUILD
                     COMMAND strip -s $<TARGET_FILE:example>
                     COMMENT "Stripping symbols and sections from example module")
endif()

install(PROGRAMS $<TARGET_FILE:example>
        DESTINATION lib/memgraph/query_modules
        RENAME example.so)
# Also install the source of the example, so user can read it.
install(FILES example.c DESTINATION lib/memgraph/query_modules/src)

# Install the Python example
install(FILES example.py DESTINATION lib/memgraph/query_modules RENAME py_example.py)

# Install the Python modules
install(FILES graph_analyzer.py DESTINATION lib/memgraph/query_modules)
install(FILES mgp_networkx.py DESTINATION lib/memgraph/query_modules)
install(FILES nxalg.py DESTINATION lib/memgraph/query_modules)
install(FILES wcc.py DESTINATION lib/memgraph/query_modules)
