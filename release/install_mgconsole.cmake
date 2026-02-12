# Unified install script to build mgconsole using tools/ci/build-mgconsole.sh
# Handles both cmake --install and CPack packaging

# Detect if running under CPack or regular install
set(IS_CPACK FALSE)
if(DEFINED CPACK_TOPLEVEL_DIRECTORY OR DEFINED CPACK_PROJECT_VERSION)
    set(IS_CPACK TRUE)
endif()

# Determine source directory
if(IS_CPACK)
    # CPack: derive from script location
    get_filename_component(SOURCE_DIR "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)
else()
    # Regular install: use CMAKE_SOURCE_DIR if available
    if(NOT DEFINED CMAKE_SOURCE_DIR OR CMAKE_SOURCE_DIR STREQUAL "")
        get_filename_component(SOURCE_DIR "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)
    else()
        set(SOURCE_DIR "${CMAKE_SOURCE_DIR}")
    endif()
endif()

# Determine build directory
if(IS_CPACK)
    # CPack: try to find from CPack variables
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
else()
    # Regular install: use CMAKE_BINARY_DIR if available
    if(NOT DEFINED CMAKE_BINARY_DIR OR CMAKE_BINARY_DIR STREQUAL "" OR CMAKE_BINARY_DIR STREQUAL CMAKE_SOURCE_DIR)
        set(BUILD_DIR "${SOURCE_DIR}/build")
    else()
        set(BUILD_DIR "${CMAKE_BINARY_DIR}")
    endif()
endif()

# Validate directories
if(NOT EXISTS "${SOURCE_DIR}/CMakeLists.txt")
    message(FATAL_ERROR "Source directory not found: ${SOURCE_DIR}")
endif()

set(BUILD_MGCONSOLE_SCRIPT "${SOURCE_DIR}/tools/ci/build-mgconsole.sh")
set(MGCONSOLE_SOURCE "${SOURCE_DIR}/build/mgconsole/bin/mgconsole")
set(MGCONSOLE_BINARY "${BUILD_DIR}/bin/mgconsole")

if(NOT DEFINED MGCONSOLE_GIT_TAG)
    set(MGCONSOLE_GIT_TAG "v1.5.0")
endif()

# Skip if mgconsole already exists (e.g., built by CPack install script or previous run)
if(EXISTS "${MGCONSOLE_BINARY}")
    message(STATUS "mgconsole already exists at ${MGCONSOLE_BINARY}, skipping build")
    return()
endif()

message(STATUS "Building mgconsole")
message(STATUS "  Script: ${BUILD_MGCONSOLE_SCRIPT}")
message(STATUS "  Source: ${SOURCE_DIR}")
message(STATUS "  Build:  ${BUILD_DIR}")
message(STATUS "  Tag:    ${MGCONSOLE_GIT_TAG}")

# Build mgconsole (unset DESTDIR to prevent installing to staging area)
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

# Copy mgconsole to expected location
if(NOT EXISTS "${MGCONSOLE_SOURCE}")
    message(FATAL_ERROR "mgconsole binary not found at ${MGCONSOLE_SOURCE}")
endif()

file(MAKE_DIRECTORY "${BUILD_DIR}/bin")
file(COPY "${MGCONSOLE_SOURCE}"
     DESTINATION "${BUILD_DIR}/bin"
     FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                      GROUP_READ GROUP_EXECUTE
                      WORLD_READ WORLD_EXECUTE)

message(STATUS "mgconsole built and copied to ${MGCONSOLE_BINARY}")
