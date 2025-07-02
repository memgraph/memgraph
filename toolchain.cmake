# This is the directory containing this toolchain file
set(MG_TOOLCHAIN_ROOT "${CMAKE_CURRENT_LIST_DIR}")

# Enable ccache
find_program(CCACHE_PROGRAM ccache)

if(CCACHE_PROGRAM)
    message(STATUS "Using ccache: ${CCACHE_PROGRAM}")
    set(CMAKE_C_COMPILER_LAUNCHER   ${CCACHE_PROGRAM} CACHE STRING "" FORCE)
    set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE_PROGRAM} CACHE STRING "" FORCE)
endif()

# Set compiler
set(CMAKE_C_COMPILER   "${MG_TOOLCHAIN_ROOT}/bin/clang"   CACHE STRING "" FORCE)
set(CMAKE_CXX_COMPILER "${MG_TOOLCHAIN_ROOT}/bin/clang++" CACHE STRING "" FORCE)

# Set linker
set(CMAKE_LINKER "${MG_TOOLCHAIN_ROOT}/bin/ld.lld" CACHE FILEPATH "" FORCE)

# Tell Clang to use the custom linker
set(CMAKE_C_FLAGS   "${CMAKE_C_FLAGS} -fuse-ld=lld")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fuse-ld=lld")
