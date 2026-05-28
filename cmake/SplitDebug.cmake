# Split-debug support: extract debug info into sidecar .debug files and keep
# the installed binary stripped. Orthogonal to CMAKE_BUILD_TYPE — only has
# something to extract when the build type produces debug info.
#
# Usage:
#   include(SplitDebug)
#   mg_split_debug(my_target [INSTALL_DESTINATION <dir>])
#
# Enable with -DMG_SPLIT_DEBUG=ON at configure time. Requires
# CMAKE_BUILD_TYPE=RelWithDebInfo or Debug.

option(MG_SPLIT_DEBUG "Extract debug info into sidecar .debug files post-link" OFF)

if(MG_SPLIT_DEBUG)
    if(NOT CMAKE_BUILD_TYPE MATCHES "^(RelWithDebInfo|Debug)$")
        message(FATAL_ERROR
            "MG_SPLIT_DEBUG requires CMAKE_BUILD_TYPE=RelWithDebInfo or Debug "
            "(current: '${CMAKE_BUILD_TYPE}'). Release has no debug info to split.")
    endif()
    if(NOT CMAKE_OBJCOPY)
        message(FATAL_ERROR "MG_SPLIT_DEBUG requires objcopy, but CMAKE_OBJCOPY is unset.")
    endif()
    message(STATUS "MG_SPLIT_DEBUG enabled (using ${CMAKE_OBJCOPY})")
endif()

# mg_split_debug(target [INSTALL_DESTINATION <dir>])
#
# POST_BUILD: extract DWARF into <target>.debug, strip the binary, and embed a
# .gnu_debuglink pointing at the sidecar.
#
# If INSTALL_DESTINATION is given, the sidecar is installed into the
# "debuginfo" CPack component, which ships in
# memgraph-debuginfo_*.deb / memgraph-debuginfo*.rpm next to the matching
# binary so gdb resolves it via .gnu_debuglink.
#
# For symbol-server prep, run `tools/ci/prepare-symbol-archive.sh` against the
# build tree; it does its own `cmake --install --component debuginfo` into a
# staging dir, then lays the files out by build-id.
#
# No-op when MG_SPLIT_DEBUG is OFF, so callers don't need to guard.
function(mg_split_debug target)
    if(NOT MG_SPLIT_DEBUG)
        return()
    endif()
    cmake_parse_arguments(MGSD "" "INSTALL_DESTINATION" "" ${ARGN})

    add_custom_command(TARGET ${target} POST_BUILD
        COMMAND ${CMAKE_OBJCOPY} --only-keep-debug --compress-debug-sections=zlib $<TARGET_FILE:${target}> $<TARGET_FILE:${target}>.debug
        # --strip-debug keeps .symtab so customer-side stack traces (journalctl,
        # perf, gdb without the debuginfo package) still resolve to function
        # names. Switch to --strip-all once a symbol server is in place and
        # sidecar-less traces stop mattering for first-contact debugging.
        COMMAND ${CMAKE_OBJCOPY} --strip-debug $<TARGET_FILE:${target}>
        COMMAND ${CMAKE_OBJCOPY} --add-gnu-debuglink=$<TARGET_FILE_NAME:${target}>.debug $<TARGET_FILE:${target}>
        WORKING_DIRECTORY $<TARGET_FILE_DIR:${target}>
        COMMENT "Splitting debug info for ${target}")

    if(MGSD_INSTALL_DESTINATION)
        install(FILES $<TARGET_FILE:${target}>.debug
            DESTINATION ${MGSD_INSTALL_DESTINATION}
            COMPONENT debuginfo
            OPTIONAL)
    endif()
endfunction()
