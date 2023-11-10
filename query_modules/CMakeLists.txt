# Memgraph Query Modules CMake configuration
# You should use the top level CMake configuration with -DQUERY_MODULES=ON
# These modules are meant to be shipped with Memgraph installation.

project(memgraph_query_modules)

disallow_in_source_build()

# Everything that is installed here, should be under the "query_modules" component.
set(CMAKE_INSTALL_DEFAULT_COMPONENT_NAME "query_modules")
string(TOLOWER ${CMAKE_BUILD_TYPE} lower_build_type)

add_library(example_c STATIC example.c)
target_include_directories(example_c PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_compile_options(example_c PRIVATE -Wall -static-libstdc++ -static-libgcc)
# Strip C example in release build.
if (lower_build_type STREQUAL "release")
  add_custom_command(TARGET example_c POST_BUILD
                     COMMAND strip -s $<TARGET_FILE:example_c>
                     COMMENT "Stripping symbols and sections from the C example module")
endif()
install(PROGRAMS $<TARGET_FILE:example_c>
        DESTINATION lib/memgraph/query_modules
        RENAME example_c.a)
# Also install the source of the example, so user can read it.
install(FILES example.c DESTINATION lib/memgraph/query_modules/src)

add_library(example_cpp STATIC example.cpp)
target_include_directories(example_cpp PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_compile_options(example_cpp PRIVATE -Wall -static-libstdc++ -static-libgcc)
# Strip C++ example in release build.
if (lower_build_type STREQUAL "release")
  add_custom_command(TARGET example_cpp POST_BUILD
                     COMMAND strip -s $<TARGET_FILE:example_cpp>
                     COMMENT "Stripping symbols and sections from the C++ example module")
endif()
install(PROGRAMS $<TARGET_FILE:example_cpp>
        DESTINATION lib/memgraph/query_modules
        RENAME example_cpp.a)
# Also install the source of the example, so user can read it.
install(FILES example.cpp DESTINATION lib/memgraph/query_modules/src)

add_library(schema STATIC schema.cpp)
target_include_directories(schema PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_compile_options(schema PRIVATE -Wall -static-libstdc++ -static-libgcc)
# Strip C++ example in release build.
if (lower_build_type STREQUAL "release")
  add_custom_command(TARGET schema POST_BUILD
                     COMMAND strip -s $<TARGET_FILE:schema>
                     COMMENT "Stripping symbols and sections from the C++ schema module")
endif()
install(PROGRAMS $<TARGET_FILE:schema>
        DESTINATION lib/memgraph/query_modules
        RENAME schema.a)
# Also install the source of the example, so user can read it.
install(FILES schema.cpp DESTINATION lib/memgraph/query_modules/src)

# Install the Python example and modules
install(FILES example.py DESTINATION lib/memgraph/query_modules RENAME py_example.py)
install(FILES graph_analyzer.py DESTINATION lib/memgraph/query_modules)
install(FILES mgp_networkx.py DESTINATION lib/memgraph/query_modules)
install(FILES nxalg.py DESTINATION lib/memgraph/query_modules)
install(FILES wcc.py DESTINATION lib/memgraph/query_modules)
install(FILES mgps.py DESTINATION lib/memgraph/query_modules)
install(FILES convert.py DESTINATION lib/memgraph/query_modules)
