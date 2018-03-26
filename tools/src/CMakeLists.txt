# CSV Import Tool Target
add_executable(mg_import_csv mg_import_csv/main.cpp)
target_link_libraries(mg_import_csv memgraph_lib)

# StatsD Target
add_executable(mg_statsd mg_statsd/main.cpp)
target_link_libraries(mg_statsd memgraph_lib)

# Strip the executable in release build.
string(TOLOWER ${CMAKE_BUILD_TYPE} lower_build_type)
if (lower_build_type STREQUAL "release")
  add_custom_command(TARGET mg_import_csv POST_BUILD
                     COMMAND strip -s mg_import_csv
                     COMMENT Stripping symbols and sections from mg_import_csv)
endif()

install(TARGETS mg_import_csv RUNTIME DESTINATION bin)

# Target for building all the tool executables.
add_custom_target(tools DEPENDS mg_import_csv mg_statsd)
