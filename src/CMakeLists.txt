# CMake configuration for the main memgraph library and executable

# add memgraph sub libraries, ordered by dependency
add_subdirectory(csv)
add_subdirectory(utils)
add_subdirectory(metrics)
add_subdirectory(parameters)
add_subdirectory(requests)
add_subdirectory(io)
add_subdirectory(kvstore)
add_subdirectory(telemetry)
add_subdirectory(communication)
add_subdirectory(memory)
add_subdirectory(storage/v2)
add_subdirectory(integrations)
add_subdirectory(query)
add_subdirectory(glue)
add_subdirectory(slk)
add_subdirectory(rpc)
add_subdirectory(license)
add_subdirectory(auth)
add_subdirectory(audit)
add_subdirectory(dbms)
add_subdirectory(flags)
add_subdirectory(distributed)
add_subdirectory(replication)
add_subdirectory(replication_handler)
add_subdirectory(coordination)
add_subdirectory(replication_coordination_glue)
add_subdirectory(system)
add_subdirectory(planner)

string(TOLOWER ${CMAKE_BUILD_TYPE} lower_build_type)

# Generate a version.hpp file
set(VERSION_STRING ${MEMGRAPH_VERSION})
configure_file(version.hpp.in version.hpp @ONLY)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# ----------------------------------------------------------------------------
# Memgraph Single Node v2 Executable
# ----------------------------------------------------------------------------
set(mg_single_node_v2_sources
        memgraph.cpp
)

# memgraph main executable
add_executable(memgraph ${mg_single_node_v2_sources})
target_include_directories(memgraph PUBLIC ${CMAKE_SOURCE_DIR}/include)
# Ensure mgconsole is built when memgraph is built
# Note: When using CPack, mgconsole is built via CPack install script instead
add_dependencies(memgraph mgconsole)

# Link memgraph with custom libstdc++
message(STATUS "Linking memgraph with custom libstdc++")
target_link_libraries(memgraph PRIVATE "$<LINK_LIBRARY:WHOLE_ARCHIVE,mg-memory>" Threads::Threads
        mg-telemetry mgcxx_text_search tantivy_text_search mg-communication mg-communication-metrics mg-utils mg-license mg-settings mg-parameters mg-glue mg-flags mg::system mg::replication_handler
        libstdc++_custom)
target_link_options(memgraph PRIVATE -nostdlib++)

# NOTE: `include/mg_procedure.syms` describes a pattern match for symbols which
# should be dynamically exported, so that `dlopen` can correctly link the
# symbols in custom procedure module libraries.
# The better approach might be to use visibility to define what to export. This is a simple way to have targeted
# exporting of symbols without having to deal with all the libraries and their visibility.
target_link_options(memgraph PRIVATE "LINKER:--dynamic-list=${CMAKE_SOURCE_DIR}/include/mg_procedure.syms")
# Set RPATH for custom libstdc++
set_target_properties(memgraph PROPERTIES
    OUTPUT_NAME "memgraph"
    INSTALL_RPATH "\$ORIGIN"
    # Output the executable in main binary dir.
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
    POSITION_INDEPENDENT_CODE ON
)

# Emulate the installed python_support, by creating a symlink
add_custom_command(TARGET memgraph POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_SOURCE_DIR}/include ${CMAKE_BINARY_DIR}/python_support
        BYPRODUCTS ${CMAKE_BINARY_DIR}/python_support
        COMMENT "Creating symlink for python_support")

# Strip the executable in release build.
if(lower_build_type STREQUAL "release")
        add_custom_command(TARGET memgraph POST_BUILD
                COMMAND strip -s $<TARGET_FILE:memgraph>
                COMMENT "Stripping symbols and sections from memgraph")
endif()

mg_split_debug(memgraph INSTALL_DESTINATION lib/memgraph)

# Apply the abi3 DT_NEEDED rewrite to the memgraph binary. See
# cmake/functions.cmake for the helper, cmake/RewriteDtNeededAbi3.cmake for
# the script that actually invokes patchelf, and src/query/CMakeLists.txt for
# where `Py_LIMITED_API` is set. The same rewrite is also applied to every
# `add_unit_test` target (see cmake/unit_tests.cmake) so the test suite
# exercises the same loader path as the deployment binary.
mg_apply_python_abi3_rewrite(memgraph)

# Emit the once-per-build configure-time warnings so the developer knows why
# the rewrite was skipped (the helper itself is silent on the skip).
find_program(PATCHELF_EXECUTABLE patchelf)
find_library(MG_LIBPYTHON3_SO python3)
if(MG_PYTHON_REWRITE_DT_NEEDED AND NOT (PATCHELF_EXECUTABLE AND MG_LIBPYTHON3_SO))
        if(NOT PATCHELF_EXECUTABLE)
                message(WARNING "patchelf not found; skipping DT_NEEDED rewrite. "
                                "Install patchelf to make memgraph binaries version-portable across Python minor versions.")
        endif()
        if(NOT MG_LIBPYTHON3_SO)
                message(WARNING "libpython3.so (unversioned abi3 SONAME) not found; skipping DT_NEEDED rewrite. "
                                "On Debian/Ubuntu, symlink it manually: "
                                "`sudo ln -sf libpython3.XX.so.1.0 /usr/lib/x86_64-linux-gnu/libpython3.so`, "
                                "or run `sudo environment/os/install_deps.sh install MEMGRAPH_BUILD_DEPS`. "
                                "Without this, binaries keep a versioned libpython dependency.")
        endif()
endif()

# Generate the configuration file under the build directory.
add_custom_command(TARGET memgraph POST_BUILD
        COMMAND ${CMAKE_SOURCE_DIR}/config/generate.py
        ${CMAKE_BINARY_DIR}/memgraph
        ${CMAKE_BINARY_DIR}/config/memgraph.conf
        BYPRODUCTS ${CMAKE_BINARY_DIR}/config/memgraph.conf
        COMMENT "Generating memgraph configuration file")
# Copy the mappings file to the build directory.
add_custom_command(TARGET memgraph POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_SOURCE_DIR}/config/mappings.json
        ${CMAKE_BINARY_DIR}/config/apoc_compatibility_mappings.json)

# TODO: Default directory permissions to 755
# NOTE: This is added in CMake 3.11, so enable it then
# set(CMAKE_INSTALL_DEFAULT_DIRECTORY_PERMISSIONS
# OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ WORLD_READ)

# Install memgraph
install(TARGETS memgraph
    RUNTIME DESTINATION lib/memgraph)

# Install custom libstdc++
get_property(CUSTOM_LIBSTDCXX_PATH GLOBAL PROPERTY MG_CUSTOM_LIBSTDCXX_PATH)
message(STATUS "Installing custom libstdc++.so.6: ${CUSTOM_LIBSTDCXX_PATH}")

# Get the directory of the custom libstdc++
get_filename_component(CUSTOM_LIBSTDCXX_DIR "${CUSTOM_LIBSTDCXX_PATH}" DIRECTORY)

# Function to collect all files in a symbolic link chain
function(collect_symlink_chain START_PATH RESULT_LIST)
    set(CURRENT_PATH "${START_PATH}")
    set(COLLECTED_FILES "")

    # Follow the symbolic link chain
    while(IS_SYMLINK "${CURRENT_PATH}")
        # Add current file to the list
        list(APPEND COLLECTED_FILES "${CURRENT_PATH}")

        # Read the symbolic link target
        execute_process(
            COMMAND readlink "${CURRENT_PATH}"
            OUTPUT_VARIABLE SYMLINK_TARGET
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )

        # Construct the full path to the target file
        get_filename_component(CURRENT_DIR "${CURRENT_PATH}" DIRECTORY)
        set(CURRENT_PATH "${CURRENT_DIR}/${SYMLINK_TARGET}")

        # Safety check to prevent infinite loops
        if(CURRENT_PATH IN_LIST COLLECTED_FILES)
            message(WARNING "Circular symbolic link detected at: ${CURRENT_PATH}")
            break()
        endif()
    endwhile()

    # Add the final file (non-symlink) to the list
    list(APPEND COLLECTED_FILES "${CURRENT_PATH}")

    # Return the collected files
    set(${RESULT_LIST} "${COLLECTED_FILES}" PARENT_SCOPE)
endfunction()

# Collect all files in the symbolic link chain
collect_symlink_chain("${CUSTOM_LIBSTDCXX_PATH}" FILES_TO_INSTALL)

# Log what we found
list(LENGTH FILES_TO_INSTALL NUM_FILES)
if(IS_SYMLINK "${CUSTOM_LIBSTDCXX_PATH}")
    message(STATUS "Found symbolic link chain with ${NUM_FILES} files:")
    foreach(FILE_PATH ${FILES_TO_INSTALL})
        message(STATUS "  - ${FILE_PATH}")
    endforeach()
else()
    message(STATUS "Installing regular file: ${CUSTOM_LIBSTDCXX_PATH}")
endif()

# Install all files in the chain
install(FILES
    ${FILES_TO_INSTALL}
    DESTINATION lib/memgraph
    PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                GROUP_READ GROUP_EXECUTE
                WORLD_READ WORLD_EXECUTE
)

install(TARGETS mg-module-support
    LIBRARY DESTINATION lib/memgraph)

# Install Python source for supporting our embedded Python.
install(FILES ${CMAKE_SOURCE_DIR}/include/mgp.py
        DESTINATION lib/memgraph/python_support)
install(FILES ${CMAKE_SOURCE_DIR}/include/mgp_mock.py
        DESTINATION lib/memgraph/python_support)
install(FILES ${CMAKE_SOURCE_DIR}/include/_mgp_mock.py
        DESTINATION lib/memgraph/python_support)

# Install the includes file for writing custom procedures in C and C++>
install(FILES ${CMAKE_SOURCE_DIR}/include/mg_procedure.h
        DESTINATION include/memgraph)
install(FILES ${CMAKE_SOURCE_DIR}/include/_mgp.hpp
        DESTINATION include/memgraph)
install(FILES ${CMAKE_SOURCE_DIR}/include/mg_exceptions.hpp
        DESTINATION include/memgraph)
install(FILES ${CMAKE_SOURCE_DIR}/include/mgp.hpp
        DESTINATION include/memgraph)

# Install the config file (must use absolute path).
install(FILES ${CMAKE_BINARY_DIR}/config/memgraph.conf
        DESTINATION /etc/memgraph RENAME memgraph.conf)
# Install the mappings file (must use absolute path).
install(FILES ${CMAKE_BINARY_DIR}/config/apoc_compatibility_mappings.json
        DESTINATION /etc/memgraph RENAME apoc_compatibility_mappings.json)

# Install logrotate configuration (must use absolute path).
install(FILES ${CMAKE_SOURCE_DIR}/release/logrotate.conf
        DESTINATION /etc/logrotate.d RENAME memgraph)

# Create empty directories for default location of lib and log.
install(CODE "file(MAKE_DIRECTORY \$ENV{DESTDIR}/var/log/memgraph
                   \$ENV{DESTDIR}/var/lib/memgraph)")

# ----------------------------------------------------------------------------
# END Memgraph Single Node v2 Executable
# ----------------------------------------------------------------------------
