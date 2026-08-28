# Compressed DWARF: store .debug_* sections deflated inside the object files
# and the linked binary. Orthogonal to CMAKE_BUILD_TYPE and to SplitDebug --
# it shrinks whatever debug info the build type asks for, wherever it ends up.
#
# Debuggers, elfutils and perf read compressed sections transparently, so this
# is invisible except in file sizes and a small link-time cost.
#
# Usage:
#   include(CompressDebug)
#
# Selected with -DMG_COMPRESS_DEBUG=<zstd|zlib|none>. "auto" picks the best
# format both the compiler and the linker actually support.

set(MG_COMPRESS_DEBUG "auto" CACHE STRING
    "Compress DWARF sections: auto, zstd, zlib or none.")
set_property(CACHE MG_COMPRESS_DEBUG PROPERTY STRINGS auto zstd zlib none)

# Probing the real toolchain is the only reliable test of whether a format can
# be produced -- a clang that cannot compress only warns, while lld hard-errors,
# so the check must link, not just compile.
function(_mg_debug_compression_works format out_var)
    set(_probe "${CMAKE_BINARY_DIR}/CMakeFiles/mg_gz_probe_${format}")
    file(WRITE "${_probe}/probe.cpp" "int main() { return 0; }\n")
    try_compile(_ok "${_probe}/build" "${_probe}/probe.cpp"
        CMAKE_FLAGS "-DCMAKE_EXE_LINKER_FLAGS=${CMAKE_EXE_LINKER_FLAGS} -gz=${format}"
        COMPILE_DEFINITIONS "-g -gz=${format} -Werror=debug-compression-unavailable"
        OUTPUT_VARIABLE _log)
    set(${out_var} ${_ok} PARENT_SCOPE)
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
endif()

if(NOT MG_COMPRESS_DEBUG_FORMAT STREQUAL "none")
    # Debug info only exists in build types that ask for it, and -gz on a
    # build with no -g is a no-op, so this needs no per-config guard.
    add_compile_options(-gz=${MG_COMPRESS_DEBUG_FORMAT})

    # The linker rewrites the merged .debug_* sections itself, so it has to be
    # told the format independently of the compiler -- but only when the debug
    # info is meant to stay in the binary.
    #
    # mg_split_debug() moves it into a sidecar with objcopy, and objcopy copies
    # an already-compressed section straight through instead of redoing it. The
    # sidecar would then carry the linker's compression, which is tuned for link
    # speed and is measurably weaker than what objcopy produces, so the shipped
    # debuginfo would grow. Which format the sidecar ends up in would also
    # depend on which objcopy CMake found: llvm-objcopy copies a zstd section
    # through, while GNU objcopy cannot read one at all and fails the build.
    # Leaving the link uncompressed lets objcopy compress the sidecar.
    if(MG_SPLIT_DEBUG)
        message(STATUS
            "MG_COMPRESS_DEBUG: link-time compression disabled; "
            "MG_SPLIT_DEBUG compresses the sidecar instead")
    else()
        add_link_options(-gz=${MG_COMPRESS_DEBUG_FORMAT})
    endif()
endif()

message(STATUS "MG_COMPRESS_DEBUG: ${MG_COMPRESS_DEBUG} -> ${MG_COMPRESS_DEBUG_FORMAT}")
