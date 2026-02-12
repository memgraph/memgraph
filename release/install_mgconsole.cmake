# Install-time script to build mgconsole using the same method as CPack
# This runs during cmake --install, before the install(FILES) command

set(BUILD_MGCONSOLE_SCRIPT "${CMAKE_SOURCE_DIR}/tools/ci/build-mgconsole.sh")
set(MGCONSOLE_SOURCE "${CMAKE_SOURCE_DIR}/build/mgconsole/bin/mgconsole")
set(MGCONSOLE_BINARY "${CMAKE_BINARY_DIR}/bin/mgconsole")

# Get MGCONSOLE_GIT_TAG if defined
if(NOT DEFINED MGCONSOLE_GIT_TAG)
    set(MGCONSOLE_GIT_TAG "v1.5.0")
endif()

message(STATUS "Building mgconsole during install stage")
message(STATUS "  Script: ${BUILD_MGCONSOLE_SCRIPT}")
message(STATUS "  Tag:    ${MGCONSOLE_GIT_TAG}")

# Build mgconsole using the same script as CPack
execute_process(
    COMMAND bash -c "unset DESTDIR && export MGCONSOLE_TAG=\"${MGCONSOLE_GIT_TAG}\" && bash \"${BUILD_MGCONSOLE_SCRIPT}\""
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
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
file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
file(COPY "${MGCONSOLE_SOURCE}"
     DESTINATION "${CMAKE_BINARY_DIR}/bin"
     FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                      GROUP_READ GROUP_EXECUTE
                      WORLD_READ WORLD_EXECUTE)

message(STATUS "mgconsole built and ready for installation")
