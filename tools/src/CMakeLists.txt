# Generate a version.hpp file
set(VERSION_STRING ${MEMGRAPH_VERSION})
configure_file(../../src/version.hpp.in version.hpp @ONLY)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

find_package(gflags REQUIRED)
find_package(fmt REQUIRED)
find_package(Threads REQUIRED)

# Memgraph Dump Target
add_executable(mg_dump mg_dump/main.cpp)
target_include_directories(mg_dump PRIVATE ${MGCLIENT_INCLUDE_DIR})
target_link_libraries(mg_dump gflags spdlog fmt::fmt mgclient Threads::Threads)
install(TARGETS mg_dump RUNTIME DESTINATION bin)

# Target for building all the tool executables.
add_custom_target(tools DEPENDS mg_dump)
