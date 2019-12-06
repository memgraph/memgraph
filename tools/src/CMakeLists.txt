# CSV Import Tool Target
add_executable(mg_import_csv mg_import_csv/main.cpp)
target_link_libraries(mg_import_csv mg-single-node kvstore_dummy_lib)

# Generate a version.hpp file
set(VERSION_STRING ${memgraph_VERSION})
configure_file(../../src/version.hpp.in version.hpp @ONLY)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# Memgraph Client Target
add_executable(mg_client mg_client/main.cpp)
set(CLIENT_LIBS mg-communication mg-io mg-utils)
if (READLINE_FOUND)
  list(APPEND CLIENT_LIBS readline)
endif()
target_link_libraries(mg_client ${CLIENT_LIBS})

# Memgraph Dump Target
add_executable(mg_dump mg_dump/main.cpp)
target_include_directories(mg_dump PRIVATE ${MGCLIENT_INCLUDE_DIR})
target_link_libraries(mg_dump fmt gflags glog mgclient pthread)

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

# TODO (mferencevic): Currently the `mg_import_csv` tool is tailored to the old
# storage and doesn't work with storage-v2.
# install(TARGETS mg_import_csv RUNTIME DESTINATION bin)
install(TARGETS mg_client RUNTIME DESTINATION bin)

# Target for building all the tool executables.
add_custom_target(tools DEPENDS mg_import_csv mg_client mg_dump)
