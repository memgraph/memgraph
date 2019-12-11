set(MAIN src/main.cpp)
set(MODULE src/louvain_module.cpp)
set(SOURCES src/algorithms/louvain.cpp
            src/data_structures/graph.cpp)

include_directories(src)

add_library(louvain-core STATIC ${SOURCES})
set_target_properties(louvain-core PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_executable(louvain-main ${MAIN})
target_link_libraries(louvain-main louvain-core)

enable_testing()
add_subdirectory(test)

add_library(louvain SHARED ${MODULE})
target_link_libraries(louvain louvain-core)
target_include_directories(louvain PRIVATE ${CMAKE_SOURCE_DIR}/include)

# Strip the library in release build.
string(TOLOWER ${CMAKE_BUILD_TYPE} lower_build_type)
if (lower_build_type STREQUAL "release")
  add_custom_command(TARGET louvain POST_BUILD
                     COMMAND strip -s $<TARGET_FILE:louvain>
                     COMMENT "Stripping symbols and sections from louvain module")
endif()

if (NOT MG_COMMUNITY)
  install(PROGRAMS $<TARGET_FILE:louvain>
          DESTINATION lib/memgraph/query_modules
          RENAME louvain.so)
endif()
