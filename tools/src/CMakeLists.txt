# Generate a version.hpp file
set(VERSION_STRING ${MEMGRAPH_VERSION})
configure_file(../../src/version.hpp.in version.hpp @ONLY)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

set(TOOLS_BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR})

include(ExternalProject)

ExternalProject_Add(mgconsole-proj
        PREFIX            mgconsole
        GIT_REPOSITORY    https://github.com/memgraph/mgconsole.git
        GIT_TAG           master
        INSTALL_DIR       ${TOOLS_BINARY_DIR}
        CMAKE_ARGS        "-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>"
                          "-DCMAKE_BUILD_TYPE=Release")

add_executable(mgconsole IMPORTED)
ExternalProject_Get_Property(mgconsole-proj install_dir)
set_target_properties(mgconsole PROPERTIES IMPORTED_LOCATION ${install_dir}/mgconsole)
add_dependencies(mgconsole mgconsole-proj)

# Memgraph Dump Target
add_executable(mg_dump mg_dump/main.cpp)
target_include_directories(mg_dump PRIVATE ${MGCLIENT_INCLUDE_DIR})
target_link_libraries(mg_dump gflags spdlog fmt mgclient pthread)
install(TARGETS mg_dump RUNTIME DESTINATION ${install_dir})

# Target for building all the tool executables.
add_custom_target(tools DEPENDS mgconsole mg_dump)
