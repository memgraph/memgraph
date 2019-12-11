set(MODULE src/connectivity_module.cpp)

include_directories(src)

add_library(connectivity SHARED ${MODULE})
target_include_directories(connectivity PRIVATE ${CMAKE_SOURCE_DIR}/include)

# Strip the library in release build.
string(TOLOWER ${CMAKE_BUILD_TYPE} lower_build_type)
if (lower_build_type STREQUAL "release")
  add_custom_command(TARGET connectivity POST_BUILD
                     COMMAND strip -s $<TARGET_FILE:connectivity>
                     COMMENT "Stripping symbols and sections from connectivity module")
endif()

install(PROGRAMS $<TARGET_FILE:connectivity>
        DESTINATION lib/memgraph/query_modules
        RENAME connectivity.so)
