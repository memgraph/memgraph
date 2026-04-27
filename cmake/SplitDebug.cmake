# Split-debug support: extract debug info into sidecar .debug files and keep
# the installed binary stripped. Orthogonal to CMAKE_BUILD_TYPE — only has
# something to extract when the build type produces debug info.
#
# Usage:
#   include(SplitDebug)
#   mg_split_debug(my_target)
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

# Attach a POST_BUILD step to `target` that extracts debug info into
# <target>.debug, strips the original, and embeds a .gnu_debuglink pointing at
# the sidecar. No-op when MG_SPLIT_DEBUG is OFF so callers don't need to guard.
function(mg_split_debug target)
    if(NOT MG_SPLIT_DEBUG)
        return()
    endif()
    add_custom_command(TARGET ${target} POST_BUILD
        # --compress-debug-sections=zlib shrinks the .debug sidecar ~3x with no
        # client-side cost: gdb/lldb/perf/addr2line all read SHF_COMPRESSED
        # transparently. Could switch to zstd for ~5-10% smaller files, but
        # toolchain-v7's llvm-objcopy is built without zstd support today.
        COMMAND ${CMAKE_OBJCOPY} --only-keep-debug --compress-debug-sections=zlib $<TARGET_FILE:${target}> $<TARGET_FILE:${target}>.debug
        # --strip-all drops .symtab/.strtab on top of DWARF, matching old Release size.
        # Switch to --strip-debug for ~20-25% larger binaries that produce nicer
        # stack traces when the .debug sidecar is unavailable.
        COMMAND ${CMAKE_OBJCOPY} --strip-all       $<TARGET_FILE:${target}>
        COMMAND ${CMAKE_OBJCOPY} --add-gnu-debuglink=$<TARGET_FILE_NAME:${target}>.debug $<TARGET_FILE:${target}>
        WORKING_DIRECTORY $<TARGET_FILE_DIR:${target}>
        COMMENT "Splitting debug info for ${target}")
endfunction()
