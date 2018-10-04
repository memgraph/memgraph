# CSV Import Tool Target
add_executable(mg_import_csv mg_import_csv/main.cpp)
target_link_libraries(mg_import_csv mg-single-node kvstore_dummy_lib)

# StatsD Target
add_executable(mg_statsd mg_statsd/main.cpp)
target_link_libraries(mg_statsd mg-communication mg-io mg-utils mg-stats)

# Memgraph Client Target
add_executable(mg_client mg_client/main.cpp)
set(CLIENT_LIBS mg-communication mg-io mg-utils)
if (READLINE_FOUND)
  list(APPEND CLIENT_LIBS readline)
endif()
target_link_libraries(mg_client ${CLIENT_LIBS})

# Strip the executable in release build.
string(TOLOWER ${CMAKE_BUILD_TYPE} lower_build_type)
if (lower_build_type STREQUAL "release")
  add_custom_command(TARGET mg_import_csv POST_BUILD
                     COMMAND strip -s mg_import_csv
                     COMMENT Stripping symbols and sections from mg_import_csv)
  add_custom_command(TARGET mg_client POST_BUILD
                     COMMAND strip -s mg_client
                     COMMENT Stripping symbols and sections from mg_client)
endif()

install(TARGETS mg_import_csv RUNTIME DESTINATION bin)
install(TARGETS mg_client RUNTIME DESTINATION bin)

# Target for building all the tool executables.
add_custom_target(tools DEPENDS mg_import_csv mg_statsd mg_client)
