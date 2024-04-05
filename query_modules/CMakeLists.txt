# Memgraph Query Modules CMake configuration
# You should use the top level CMake configuration with -DQUERY_MODULES=ON
# These modules are meant to be shipped with Memgraph installation.

project(memgraph_query_modules)

disallow_in_source_build()

find_package(fmt REQUIRED)

# Everything that is installed here, should be under the "query_modules" component.
set(CMAKE_INSTALL_DEFAULT_COMPONENT_NAME "query_modules")
string(TOLOWER ${CMAKE_BUILD_TYPE} lower_build_type)

add_library(example_c SHARED example.c)
target_include_directories(example_c PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_compile_options(example_c PRIVATE -Wall)
target_link_libraries(example_c PRIVATE -static-libgcc -static-libstdc++)
# Strip C example in release build.
if (lower_build_type STREQUAL "release")
  add_custom_command(TARGET example_c POST_BUILD
                     COMMAND strip -s $<TARGET_FILE:example_c>
                     COMMENT "Stripping symbols and sections from the C example module")
endif()
install(PROGRAMS $<TARGET_FILE:example_c>
        DESTINATION lib/memgraph/query_modules
        RENAME example_c.so)
# Also install the source of the example, so user can read it.
install(FILES example.c DESTINATION lib/memgraph/query_modules/src)

add_library(example_cpp SHARED example.cpp)
target_include_directories(example_cpp PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_compile_options(example_cpp PRIVATE -Wall)
target_link_libraries(example_cpp PRIVATE -static-libgcc -static-libstdc++)
# Strip C++ example in release build.
if (lower_build_type STREQUAL "release")
  add_custom_command(TARGET example_cpp POST_BUILD
                     COMMAND strip -s $<TARGET_FILE:example_cpp>
                     COMMENT "Stripping symbols and sections from the C++ example module")
endif()
install(PROGRAMS $<TARGET_FILE:example_cpp>
        DESTINATION lib/memgraph/query_modules
        RENAME example_cpp.so)
# Also install the source of the example, so user can read it.
install(FILES example.cpp DESTINATION lib/memgraph/query_modules/src)

add_library(schema SHARED schema.cpp)
target_include_directories(schema PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_compile_options(schema PRIVATE -Wall)
target_link_libraries(schema PRIVATE -static-libgcc -static-libstdc++)
# Strip C++ example in release build.
if (lower_build_type STREQUAL "release")
  add_custom_command(TARGET schema POST_BUILD
                     COMMAND strip -s $<TARGET_FILE:schema>
                     COMMENT "Stripping symbols and sections from the C++ schema module")
endif()
install(PROGRAMS $<TARGET_FILE:schema>
        DESTINATION lib/memgraph/query_modules
        RENAME schema.so)
# Also install the source of the example, so user can read it.
install(FILES schema.cpp DESTINATION lib/memgraph/query_modules/src)

add_library(text_search SHARED text_search_module.cpp)
set_target_properties(text_search PROPERTIES PREFIX "")
target_include_directories(text_search PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_compile_options(text_search PRIVATE -Wall)
target_link_libraries(text_search PRIVATE -static-libgcc -static-libstdc++ fmt::fmt)
# Strip C++ example in release build.
if (lower_build_type STREQUAL "release")
  add_custom_command(TARGET text_search POST_BUILD
                     COMMAND strip -s $<TARGET_FILE:text_search>
                     COMMENT "Stripping symbols and sections from the C++ text_search module")
endif()
install(PROGRAMS $<TARGET_FILE:text_search>
        DESTINATION lib/memgraph/query_modules
        RENAME text_search.so)
# Also install the source of the example, so user can read it.
install(FILES text_search_module.cpp DESTINATION lib/memgraph/query_modules/src)

# Install the Python example and modules
install(FILES example.py DESTINATION lib/memgraph/query_modules RENAME py_example.py)
install(FILES graph_analyzer.py DESTINATION lib/memgraph/query_modules)
install(FILES mgp_networkx.py DESTINATION lib/memgraph/query_modules)
install(FILES nxalg.py DESTINATION lib/memgraph/query_modules)
install(FILES wcc.py DESTINATION lib/memgraph/query_modules)
install(FILES mgps.py DESTINATION lib/memgraph/query_modules)
install(FILES convert.py DESTINATION lib/memgraph/query_modules)
