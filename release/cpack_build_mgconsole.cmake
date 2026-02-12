# CPack install script to build mgconsole using tools/ci/build-mgconsole.sh
# This script runs during CPack packaging phase.
#
# Note: When CPack runs install scripts, CMAKE_SOURCE_DIR and CMAKE_BINARY_DIR
# may not be set, so we derive them from this script's location.

get_filename_component(SOURCE_DIR "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)

# Determine build directory from CPack variables or use standard layout
if(DEFINED CPACK_TOPLEVEL_DIRECTORY)
    get_filename_component(CPACK_PACKAGE_DIR "${CPACK_TOPLEVEL_DIRECTORY}" DIRECTORY)
    if(EXISTS "${CPACK_PACKAGE_DIR}/CPackConfig.cmake")
        get_filename_component(BUILD_DIR "${CPACK_PACKAGE_DIR}" ABSOLUTE)
    else()
        set(BUILD_DIR "${SOURCE_DIR}/build")
    endif()
else()
    set(BUILD_DIR "${SOURCE_DIR}/build")
endif()

# Validate directories
if(NOT EXISTS "${SOURCE_DIR}/CMakeLists.txt")
    message(FATAL_ERROR "Source directory not found: ${SOURCE_DIR}")
endif()

set(BUILD_MGCONSOLE_SCRIPT "${SOURCE_DIR}/tools/ci/build-mgconsole.sh")
if(NOT DEFINED MGCONSOLE_GIT_TAG)
    set(MGCONSOLE_GIT_TAG "v1.5.0")
endif()

message(STATUS "Building mgconsole")
message(STATUS "  Script: ${BUILD_MGCONSOLE_SCRIPT}")
message(STATUS "  Source: ${SOURCE_DIR}")
message(STATUS "  Build:  ${BUILD_DIR}")
message(STATUS "  Tag:    ${MGCONSOLE_GIT_TAG}")

# Build mgconsole
# CRITICAL: Unset DESTDIR to prevent mgconsole from installing to CPack staging.
# The build must be isolated and install to its own directory.
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

# Copy mgconsole binary to expected location
# build-mgconsole.sh installs to: ${SOURCE_DIR}/build/mgconsole/bin/mgconsole
# We need it at: ${BUILD_DIR}/bin/mgconsole
set(mgconsole_source "${SOURCE_DIR}/build/mgconsole/bin/mgconsole")
set(mgconsole_dest "${BUILD_DIR}/bin/mgconsole")

if(NOT EXISTS "${mgconsole_source}")
    message(FATAL_ERROR "mgconsole binary not found at ${mgconsole_source}")
endif()

file(MAKE_DIRECTORY "${BUILD_DIR}/bin")
file(COPY "${mgconsole_source}"
     DESTINATION "${BUILD_DIR}/bin"
     FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                      GROUP_READ GROUP_EXECUTE
                      WORLD_READ WORLD_EXECUTE)

message(STATUS "mgconsole built and copied to ${mgconsole_dest}")
