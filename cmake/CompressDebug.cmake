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

# zstd decompresses several times faster than zlib at a better ratio, but
# needs an LLVM built against libzstd; zlib support is unconditional. Probing
# the real toolchain is the only reliable test -- a clang that cannot compress
# only warns, while lld hard-errors, so the check must link, not just compile.
function(_mg_debug_compression_works format out_var)
    set(_probe "${CMAKE_BINARY_DIR}/CMakeFiles/mg_gz_probe_${format}")
    file(WRITE "${_probe}/probe.cpp" "int main() { return 0; }\n")
    try_compile(_ok "${_probe}/build" "${_probe}/probe.cpp"
        CMAKE_FLAGS "-DCMAKE_EXE_LINKER_FLAGS=${CMAKE_EXE_LINKER_FLAGS} -gz=${format}"
        COMPILE_DEFINITIONS "-g -gz=${format} -Werror=debug-compression-unavailable"
        OUTPUT_VARIABLE _log)
    set(${out_var} ${_ok} PARENT_SCOPE)
endfunction()

if(MG_COMPRESS_DEBUG STREQUAL "auto")
    _mg_debug_compression_works(zstd _mg_have_zstd)
    if(_mg_have_zstd)
        set(MG_COMPRESS_DEBUG_FORMAT zstd)
    else()
        _mg_debug_compression_works(zlib _mg_have_zlib)
        if(_mg_have_zlib)
            set(MG_COMPRESS_DEBUG_FORMAT zlib)
        else()
            set(MG_COMPRESS_DEBUG_FORMAT none)
        endif()
    endif()
else()
    set(MG_COMPRESS_DEBUG_FORMAT ${MG_COMPRESS_DEBUG})
endif()

if(NOT MG_COMPRESS_DEBUG_FORMAT STREQUAL "none")
    # Debug info only exists in build types that ask for it, and -gz on a
    # build with no -g is a no-op, so this needs no per-config guard.
    add_compile_options(-gz=${MG_COMPRESS_DEBUG_FORMAT})
    # The linker rewrites the merged .debug_* sections itself, so it has to be
    # told the format independently of the compiler.
    add_link_options(-gz=${MG_COMPRESS_DEBUG_FORMAT})
endif()

message(STATUS "MG_COMPRESS_DEBUG: ${MG_COMPRESS_DEBUG} -> ${MG_COMPRESS_DEBUG_FORMAT}")
