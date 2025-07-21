# Generate a version.hpp file
set(VERSION_STRING ${MEMGRAPH_VERSION})
configure_file(../../src/version.hpp.in version.hpp @ONLY)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

find_package(gflags REQUIRED)
find_package(fmt REQUIRED)
find_package(Threads REQUIRED)

# Memgraph Dump Target
add_executable(mg_dump mg_dump/main.cpp)

# Link mg_dump with custom libstdc++
message(STATUS "Linking mg_dump with custom libstdc++")
target_link_libraries(mg_dump gflags spdlog fmt::fmt mgclient Threads::Threads libstdc++_custom)
target_link_options(mg_dump PRIVATE -nostdlib++)

# Set RPATH for custom libstdc++
set_target_properties(mg_dump PROPERTIES
    INSTALL_RPATH "\$ORIGIN/../lib/memgraph"
    INSTALL_RPATH_USE_LINK_PATH TRUE
)

install(TARGETS mg_dump RUNTIME DESTINATION bin)

# Target for building all the tool executables.
add_custom_target(tools DEPENDS mg_dump)
