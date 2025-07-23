# Memgraph Query Modules CMake configuration
# You should use the top level CMake configuration with -DQUERY_MODULES=ON
# These modules are meant to be shipped with Memgraph installation.

project(memgraph_query_modules)

disallow_in_source_build()

find_package(fmt REQUIRED)

# Everything that is installed here, should be under the "query_modules" component.
set(CMAKE_INSTALL_DEFAULT_COMPONENT_NAME "query_modules")
string(TOLOWER ${CMAKE_BUILD_TYPE} lower_build_type)

function(add_post_build_strip_in_release target build_type_var)
  string(TOLOWER "${${build_type_var}}" lower_build_type)

  if (lower_build_type STREQUAL "release")
    add_custom_command(
      TARGET ${target} POST_BUILD
      COMMAND ${CMAKE_STRIP} -s $<TARGET_FILE:${target}>
      COMMENT "Stripping symbols and sections from the ${target} module"
    )
  endif()
endfunction()

function(add_custom_library NAME SRC)
    set(optional_libs ${ARGN})

    add_library(${NAME} SHARED ${SRC})
    target_include_directories(${NAME} PRIVATE ${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/libs)
    target_compile_options(${NAME} PRIVATE -Wall)
    target_link_libraries(${NAME} PRIVATE ${optional_libs})

    # Apply post-build strip in release mode
    add_post_build_strip_in_release(${NAME} CMAKE_BUILD_TYPE)

    set_target_properties(${NAME} PROPERTIES
        PREFIX ""
        OUTPUT_NAME "${NAME}"
    )

    # Also install the source of the example, so user can read it.
    install(FILES ${SRC} DESTINATION lib/memgraph/query_modules/src)
endfunction()

add_custom_library(example_c example.c)
add_custom_library(example_cpp example.cpp)
add_custom_library(schema schema.cpp)
add_custom_library(text_search text_search_module.cpp fmt::fmt)
add_custom_library(vector_search vector_search_module.cpp fmt::fmt)
add_custom_library(convert convert.cpp fmt::fmt nlohmann_json::nlohmann_json)


set(QUERY_MODULES_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}" CACHE INTERNAL "Build directory for QMs in folders")
set(QUERY_MODULES_DIR lib/memgraph/query_modules)

function(add_query_module target_name version src)
  add_library(${target_name} SHARED ${src})
  set_property(TARGET ${target_name} PROPERTY POSITION_INDEPENDENT_CODE TRUE)
  target_include_directories(${target_name} PRIVATE
      ${CMAKE_SOURCE_DIR}/include
      ${CMAKE_CURRENT_SOURCE_DIR}
  )

  target_compile_options(${target_name} PRIVATE -Wall)

  add_post_build_strip_in_release(${target_name} CMAKE_BUILD_TYPE)

  set_target_properties(${target_name} PROPERTIES
      PREFIX "" # remove "lib" prefix
      OUTPUT_NAME "${target_name}"
      LIBRARY_OUTPUT_DIRECTORY "${QUERY_MODULES_BUILD_DIR}" # needed because we want all .so in /build/query_modules
  )
endfunction()

add_subdirectory(pagerank_module)
add_subdirectory(katz_centrality_module)
add_subdirectory(community_detection_module)

# Copy python query modules into build/query_modules
# This is to avoid doing make install during CI
# C and C++ modules .so files will already be there
file(MAKE_DIRECTORY ${QUERY_MODULES_BUILD_DIR})

# node2vec gets copied inside subdirectory
# TODO: make this consistent
add_subdirectory(node2vec_online_module)

set(PYTHON_QUERY_MODULES_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/graph_analyzer.py
    ${CMAKE_CURRENT_SOURCE_DIR}/mgp_networkx.py
    ${CMAKE_CURRENT_SOURCE_DIR}/nxalg.py
    ${CMAKE_CURRENT_SOURCE_DIR}/wcc.py
    ${CMAKE_CURRENT_SOURCE_DIR}/mgps.py
    ${CMAKE_CURRENT_SOURCE_DIR}/date.py
)
add_custom_target(copy_python_query_modules ALL
    COMMAND ${CMAKE_COMMAND} -E copy ${PYTHON_QUERY_MODULES_FILES} ${QUERY_MODULES_BUILD_DIR}
    COMMENT "Copying Python query modules to build/query_modules"
)

# Install C++ query modules
install(TARGETS convert example_c example_cpp schema text_search vector_search pagerank_online
    community_detection_online katz_centrality_online
    DESTINATION lib/memgraph/query_modules
)

# Install the Python example and modules
install(FILES example.py DESTINATION lib/memgraph/query_modules RENAME py_example.py)
install(FILES graph_analyzer.py DESTINATION lib/memgraph/query_modules)
install(FILES mgp_networkx.py DESTINATION lib/memgraph/query_modules)
install(FILES nxalg.py DESTINATION lib/memgraph/query_modules)
install(FILES wcc.py DESTINATION lib/memgraph/query_modules)
install(FILES mgps.py DESTINATION lib/memgraph/query_modules)
install(FILES date.py DESTINATION lib/memgraph/query_modules)

# IMPORTANT: when adding enterprise module add its name to kEnterpriseModuleList in src/query/procedure/module.cpp
