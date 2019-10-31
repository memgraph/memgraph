# Memgraph Query Modules CMake configuration
# You should use the top level CMake configuration with -DQUERY_MODULES=ON
# These modules are meant to be shipped with Memgraph installation.

project(memgraph_query_modules VERSION ${memgraph_VERSION})

disallow_in_source_build()

# Everything that is installed here, should be under the "query_modules" component.
set(CMAKE_INSTALL_DEFAULT_COMPONENT_NAME "query_modules")

add_library(example SHARED example.c)
target_include_directories(example PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_compile_options(example PRIVATE -Wall)
install(PROGRAMS $<TARGET_FILE:example>
        DESTINATION lib/memgraph/query_modules
        RENAME example.so)
# Also install the source of the example, so user can read it.
install(FILES example.c DESTINATION lib/memgraph/query_modules)
