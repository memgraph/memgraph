# Install-time script to build mgconsole using the same method as CPack
# This runs during cmake --install, before the install(FILES) command
#
# Note: During install, CMAKE_SOURCE_DIR and CMAKE_BINARY_DIR should be available,
# but we validate them to ensure they're correct

# Validate and set source directory
if(NOT DEFINED CMAKE_SOURCE_DIR OR CMAKE_SOURCE_DIR STREQUAL "")
    # Fallback: derive from this script's location
    get_filename_component(SCRIPT_DIR "${CMAKE_CURRENT_LIST_DIR}" ABSOLUTE)
    get_filename_component(SOURCE_DIR "${SCRIPT_DIR}/.." ABSOLUTE)
else()
    set(SOURCE_DIR "${CMAKE_SOURCE_DIR}")
endif()

# Validate and set build directory
# During cmake --install, CMAKE_BINARY_DIR should be set to the build directory
if(NOT DEFINED CMAKE_BINARY_DIR OR CMAKE_BINARY_DIR STREQUAL "")
    # Fallback: assume standard layout
    set(BUILD_DIR "${SOURCE_DIR}/build")
else()
    # Use CMAKE_BINARY_DIR as-is (it should be the build directory)
    set(BUILD_DIR "${CMAKE_BINARY_DIR}")
    # If CMAKE_BINARY_DIR is the same as SOURCE_DIR, it's likely wrong - use standard layout
    if(BUILD_DIR STREQUAL SOURCE_DIR)
        set(BUILD_DIR "${SOURCE_DIR}/build")
    endif()
endif()

# Validate directories
if(NOT EXISTS "${SOURCE_DIR}/CMakeLists.txt")
    message(FATAL_ERROR "Source directory not found: ${SOURCE_DIR}")
endif()

set(BUILD_MGCONSOLE_SCRIPT "${SOURCE_DIR}/tools/ci/build-mgconsole.sh")
set(MGCONSOLE_SOURCE "${SOURCE_DIR}/build/mgconsole/bin/mgconsole")
set(MGCONSOLE_BINARY "${BUILD_DIR}/bin/mgconsole")

# Get MGCONSOLE_GIT_TAG if defined
if(NOT DEFINED MGCONSOLE_GIT_TAG)
    set(MGCONSOLE_GIT_TAG "v1.5.0")
endif()

message(STATUS "Building mgconsole during install stage")
message(STATUS "  Script: ${BUILD_MGCONSOLE_SCRIPT}")
message(STATUS "  Source: ${SOURCE_DIR}")
message(STATUS "  Build:  ${BUILD_DIR}")
message(STATUS "  Tag:    ${MGCONSOLE_GIT_TAG}")

# Build mgconsole using the same script as CPack
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

# Copy mgconsole to the expected location for install
file(MAKE_DIRECTORY "${BUILD_DIR}/bin")
file(COPY "${MGCONSOLE_SOURCE}"
     DESTINATION "${BUILD_DIR}/bin"
     FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                      GROUP_READ GROUP_EXECUTE
                      WORLD_READ WORLD_EXECUTE)

message(STATUS "mgconsole built and ready for installation")
