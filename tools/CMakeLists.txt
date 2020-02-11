# MemGraph Tools CMake configuration
# You should use the top level CMake configuration with -DTOOLS=ON option set.

project(memgraph_tools)

disallow_in_source_build()

# Everything that is installed here, should be under the "tools" component.
set(CMAKE_INSTALL_DEFAULT_COMPONENT_NAME "tools")

add_subdirectory(src)
