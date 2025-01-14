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
set_target_properties(example_c PROPERTIES
    PREFIX ""
    OUTPUT_NAME "example_c"
)
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
set_target_properties(example_cpp PROPERTIES
    PREFIX ""
    OUTPUT_NAME "example_cpp"
)
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
set_target_properties(schema PROPERTIES
    PREFIX ""
    OUTPUT_NAME "schema"
)
# Also install the source of the example, so user can read it.
install(FILES schema.cpp DESTINATION lib/memgraph/query_modules/src)

add_library(text_search SHARED text_search_module.cpp)
target_include_directories(text_search PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_compile_options(text_search PRIVATE -Wall)
target_link_libraries(text_search PRIVATE -static-libgcc -static-libstdc++ fmt::fmt)
# Strip C++ example in release build.
if (lower_build_type STREQUAL "release")
  add_custom_command(TARGET text_search POST_BUILD
                     COMMAND strip -s $<TARGET_FILE:text_search>
                     COMMENT "Stripping symbols and sections from the C++ text_search module")
endif()
set_target_properties(text_search PROPERTIES
    PREFIX ""
    OUTPUT_NAME "text_search"
)
# Also install the source of the example, so user can read it.
install(FILES text_search_module.cpp DESTINATION lib/memgraph/query_modules/src)

add_library(vector_search SHARED vector_search_module.cpp)
target_include_directories(vector_search PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_compile_options(vector_search PRIVATE -Wall)
target_link_libraries(vector_search PRIVATE -static-libgcc -static-libstdc++ fmt::fmt)
# Strip C++ example in release build.
if (lower_build_type STREQUAL "release")
  add_custom_command(TARGET vector_search POST_BUILD
                     COMMAND strip -s $<TARGET_FILE:vector_search>
                     COMMENT "Stripping symbols and sections from the C++ vector_search module")
endif()
set_target_properties(vector_search PROPERTIES
    PREFIX ""
    OUTPUT_NAME "vector_search"
)
# Also install the source of the example, so user can read it.
install(FILES vector_search_module.cpp DESTINATION lib/memgraph/query_modules/src)

add_library(convert SHARED convert.cpp)
target_include_directories(convert PRIVATE ${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/libs)
target_compile_options(convert PRIVATE -Wall)
target_link_libraries(convert PRIVATE -static-libgcc -static-libstdc++ fmt::fmt json)

# Strip the module in release build
if (lower_build_type STREQUAL "release")
  add_custom_command(TARGET convert POST_BUILD
                     COMMAND strip -s $<TARGET_FILE:convert>
                     COMMENT "Stripping symbols and sections from the C++ convert module")
endif()

set_target_properties(convert PROPERTIES
    PREFIX ""
    OUTPUT_NAME "convert"
)

# Copy python query modules into build/query_modules
# This is to avoid doing make install during CI
# C and C++ modules .so files will already be there
set(BUILD_QUERY_MODULES_DIR ${CMAKE_BINARY_DIR}/query_modules)
file(MAKE_DIRECTORY ${BUILD_QUERY_MODULES_DIR})

set(PYTHON_QUERY_MODULES_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/graph_analyzer.py
    ${CMAKE_CURRENT_SOURCE_DIR}/mgp_networkx.py
    ${CMAKE_CURRENT_SOURCE_DIR}/nxalg.py
    ${CMAKE_CURRENT_SOURCE_DIR}/wcc.py
    ${CMAKE_CURRENT_SOURCE_DIR}/mgps.py
)
add_custom_target(copy_python_query_modules ALL
    COMMAND ${CMAKE_COMMAND} -E copy ${PYTHON_QUERY_MODULES_FILES} ${BUILD_QUERY_MODULES_DIR}
    COMMENT "Copying Python query modules to build/query_modules"
)


install(FILES convert.cpp DESTINATION lib/memgraph/query_modules/src)

# Install C++ query modules
install(TARGETS convert example_c example_cpp schema text_search vector_search
    DESTINATION lib/memgraph/query_modules
)

# Install the Python example and modules
install(FILES example.py DESTINATION lib/memgraph/query_modules RENAME py_example.py)
install(FILES graph_analyzer.py DESTINATION lib/memgraph/query_modules)
install(FILES mgp_networkx.py DESTINATION lib/memgraph/query_modules)
install(FILES nxalg.py DESTINATION lib/memgraph/query_modules)
install(FILES wcc.py DESTINATION lib/memgraph/query_modules)
install(FILES mgps.py DESTINATION lib/memgraph/query_modules)

# IMPORTANT: when adding enterprise module add its name to kEnterpriseModuleList in src/query/procedure/module.cpp
