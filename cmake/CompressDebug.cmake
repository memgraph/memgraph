# Compressed DWARF: store .debug_* sections deflated inside the object files
# and the linked binary. Orthogonal to CMAKE_BUILD_TYPE -- it shrinks whatever
# debug info the build type asks for, wherever it ends up.
#
# Debuggers, elfutils and perf read compressed sections transparently, so this
# is invisible except in file sizes and a small link-time cost.
#
# Usage:
#   include(CompressDebug)
#
# Selected with -DMG_COMPRESS_DEBUG=<zstd|zlib|none>. "auto" picks the format
# every toolchain here can both produce and read back.
#
# This module owns the format decision; mg_split_debug() reads
# MG_COMPRESS_DEBUG_FORMAT so a sidecar is compressed the same way, by objcopy.

set(MG_COMPRESS_DEBUG "auto" CACHE STRING
    "Compress DWARF sections: auto, zstd, zlib or none.")
set_property(CACHE MG_COMPRESS_DEBUG PROPERTY STRINGS auto zstd zlib none)

# Probing the real toolchain is the only reliable test of whether a format can
# be produced -- a clang that cannot compress only warns, while lld hard-errors,
# so the check must link, not just compile. With MG_SPLIT_DEBUG the sidecar is
# compressed by objcopy, so it has to speak the format as well.
function(_mg_debug_compression_works format out_var)
    set(_probe "${CMAKE_BINARY_DIR}/CMakeFiles/mg_gz_probe_${format}")
    set(_result _mg_gz_probe_ok_${format})
    # try_compile caches its result; force a rerun if the probe binary is gone.
    if(NOT EXISTS "${_probe}/probe")
        unset(${_result} CACHE)
    endif()
    file(WRITE "${_probe}/probe.cpp" "int main() { return 0; }\n")
    try_compile(${_result} "${_probe}/build" "${_probe}/probe.cpp"
        CMAKE_FLAGS "-DCMAKE_EXE_LINKER_FLAGS=${CMAKE_EXE_LINKER_FLAGS} -gz=${format}"
        COMPILE_DEFINITIONS "-g -gz=${format} -Werror=debug-compression-unavailable"
        COPY_FILE "${_probe}/probe"
        OUTPUT_VARIABLE _log)
    set(_ok ${${_result}})
    if(_ok AND MG_SPLIT_DEBUG)
        execute_process(
            COMMAND ${CMAKE_OBJCOPY} --only-keep-debug --compress-debug-sections=${format}
                    "${_probe}/probe" "${_probe}/probe.debug"
            RESULT_VARIABLE _rc OUTPUT_VARIABLE _log ERROR_VARIABLE _log)
        if(NOT _rc EQUAL 0)
            set(_ok FALSE)
            string(PREPEND _log "${CMAKE_OBJCOPY} cannot produce ${format}-compressed sections:\n")
        endif()
    endif()
    set(${out_var} ${_ok} PARENT_SCOPE)
    set(${out_var}_LOG "${_log}" PARENT_SCOPE)
endfunction()

# auto picks zlib, not the best format the compiler can emit. Producing a
# format is not the same as being able to read it back: a toolchain whose gdb
# was configured without zstd rejects a zstd-compressed binary outright, as "not
# in executable format", rather than reporting missing symbols. zlib is readable
# by every toolchain here and gives up little, so it is what auto selects; ask
# for zstd explicitly when the toolchain's debugger is known to read it.
if(MG_COMPRESS_DEBUG STREQUAL "auto")
    _mg_debug_compression_works(zlib _mg_have_zlib)
    if(_mg_have_zlib)
        set(MG_COMPRESS_DEBUG_FORMAT zlib)
    else()
        set(MG_COMPRESS_DEBUG_FORMAT none)
    endif()
else()
    set(MG_COMPRESS_DEBUG_FORMAT ${MG_COMPRESS_DEBUG})
    # An explicit format is a promise the toolchain has to keep; better to
    # learn at configure time than from a failed link or post-build step.
    if(NOT MG_COMPRESS_DEBUG_FORMAT STREQUAL "none")
        _mg_debug_compression_works(${MG_COMPRESS_DEBUG_FORMAT} _mg_have_format)
        if(NOT _mg_have_format)
            message(FATAL_ERROR
                "MG_COMPRESS_DEBUG=${MG_COMPRESS_DEBUG}: this toolchain cannot produce "
                "${MG_COMPRESS_DEBUG}-compressed debug sections.\n${_mg_have_format_LOG}")
        endif()
    endif()
endif()

if(NOT MG_COMPRESS_DEBUG_FORMAT STREQUAL "none")
    # Debug info only exists in build types that ask for it, and -gz on a
    # build with no -g is a no-op, so this needs no per-config guard.
    add_compile_options(-gz=${MG_COMPRESS_DEBUG_FORMAT})

    # The linker rewrites the merged .debug_* sections itself, so it has to be
    # told the format independently of the compiler. Its compression is tuned
    # for link speed (zlib level 1, ~13% larger than objcopy's default), which
    # is the right trade for binaries that only live in the build tree.
    # Targets that ship a sidecar override this with -gz=none in
    # mg_split_debug() so objcopy compresses the sidecar itself.
    add_link_options(-gz=${MG_COMPRESS_DEBUG_FORMAT})
endif()

message(STATUS "MG_COMPRESS_DEBUG: ${MG_COMPRESS_DEBUG} -> ${MG_COMPRESS_DEBUG_FORMAT}")
