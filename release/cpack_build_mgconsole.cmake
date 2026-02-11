# CPack install script to build mgconsole using tools/ci/build-mgconsole.sh
# This script runs during CPack packaging phase
#
# Note: When CPack runs install scripts, CMAKE_SOURCE_DIR and CMAKE_BINARY_DIR
# may not be set. We derive them from this script's location.

# This script is at release/cpack_build_mgconsole.cmake
# So the source directory is the parent of release/
get_filename_component(SCRIPT_DIR "${CMAKE_CURRENT_LIST_DIR}" ABSOLUTE)
get_filename_component(SOURCE_DIR "${SCRIPT_DIR}/.." ABSOLUTE)

# The build directory is typically ${SOURCE_DIR}/build
# But we can also try to get it from CPACK_TOPLEVEL_DIRECTORY if available
if(DEFINED CPACK_TOPLEVEL_DIRECTORY)
    get_filename_component(CPACK_BUILD_DIR "${CPACK_TOPLEVEL_DIRECTORY}" DIRECTORY)
    # CPACK_TOPLEVEL_DIRECTORY is typically _CPack_Packages/.../package-name
    # So we need to go up to find the actual build directory
    # Try common patterns
    if(EXISTS "${CPACK_BUILD_DIR}/../CPackConfig.cmake")
        get_filename_component(BUILD_DIR "${CPACK_BUILD_DIR}/.." ABSOLUTE)
    elseif(EXISTS "${SOURCE_DIR}/build")
        set(BUILD_DIR "${SOURCE_DIR}/build")
    else()
        # Fallback: assume build is a sibling of source or in source/build
        set(BUILD_DIR "${CPACK_BUILD_DIR}")
    endif()
else()
    # Fallback: assume standard layout
    if(EXISTS "${SOURCE_DIR}/build")
        set(BUILD_DIR "${SOURCE_DIR}/build")
    else()
        message(FATAL_ERROR "Cannot determine build directory. Please set BUILD_DIR or ensure standard layout.")
    endif()
endif()

set(BUILD_MGCONSOLE_SCRIPT "${SOURCE_DIR}/tools/ci/build-mgconsole.sh")

# Get the MGCONSOLE_GIT_TAG if defined, otherwise use default
if(NOT DEFINED MGCONSOLE_GIT_TAG)
    set(MGCONSOLE_GIT_TAG "v1.5.0")
endif()

message(STATUS "Building mgconsole using ${BUILD_MGCONSOLE_SCRIPT}")
message(STATUS "Source directory: ${SOURCE_DIR}")
message(STATUS "Build directory: ${BUILD_DIR}")
message(STATUS "mgconsole tag: ${MGCONSOLE_GIT_TAG}")

# CRITICAL: Unset DESTDIR to prevent mgconsole build from installing to CPack staging
# The mgconsole build should be completely isolated and install to its own directory.
# CPack sets DESTDIR, which causes cmake --install to install to DESTDIR + prefix.
# We need to run the build script in an environment without DESTDIR.

# Run the build script from the source directory with DESTDIR explicitly unset
# The script calculates paths relative to its location:
# - ROOT_DIR="$SCRIPT_DIR/../../" (which should be SOURCE_DIR)
# - MG_BUILD_DIR="$ROOT_DIR/build" (which should be BUILD_DIR)
# - Installs to $MG_BUILD_DIR/mgconsole/bin/mgconsole
execute_process(
    COMMAND bash -c "unset DESTDIR && export MGCONSOLE_TAG=\"${MGCONSOLE_GIT_TAG}\" && bash \"${BUILD_MGCONSOLE_SCRIPT}\""
    WORKING_DIRECTORY "${SOURCE_DIR}"
    RESULT_VARIABLE build_result
    OUTPUT_VARIABLE build_output
    ERROR_VARIABLE build_error
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_STRIP_TRAILING_WHITESPACE
)

if(NOT build_result EQUAL 0)
    message(FATAL_ERROR "Failed to build mgconsole:\nSTDOUT: ${build_output}\nSTDERR: ${build_error}")
endif()

message(STATUS "mgconsole build completed successfully")

# The build-mgconsole.sh script installs to ${SOURCE_DIR}/build/mgconsole/bin/mgconsole
# We need to copy it to ${BUILD_DIR}/bin/mgconsole where the install step expects it
set(mgconsole_source "${SOURCE_DIR}/build/mgconsole/bin/mgconsole")
set(mgconsole_dest "${BUILD_DIR}/bin/mgconsole")

# Check if mgconsole was built
if(NOT EXISTS "${mgconsole_source}")
    message(FATAL_ERROR "mgconsole binary not found at ${mgconsole_source}")
endif()

# Ensure the destination directory exists
file(MAKE_DIRECTORY "${BUILD_DIR}/bin")

# Copy mgconsole to the expected location
file(COPY "${mgconsole_source}"
     DESTINATION "${BUILD_DIR}/bin"
     FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                      GROUP_READ GROUP_EXECUTE
                      WORLD_READ WORLD_EXECUTE)
message(STATUS "Copied mgconsole from ${mgconsole_source} to ${mgconsole_dest}")
