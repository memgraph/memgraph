set(CMAKE_SYSTEM_NAME Linux)

# NOTE: if we want to be able to cross-compile, this will need to change
execute_process(
     COMMAND uname -m
     OUTPUT_VARIABLE uname_result
     OUTPUT_STRIP_TRAILING_WHITESPACE
)
set(CMAKE_SYSTEM_PROCESSOR "${uname_result}")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(MG_TOOLCHAIN_ROOT "$ENV{MG_TOOLCHAIN_ROOT}")
message(STATUS "Toolchain directory: ${MG_TOOLCHAIN_ROOT}")

# Paths for find_package(), find_library(), etc.
set(CMAKE_FIND_ROOT_PATH "${CMAKE_FIND_ROOT_PATH};${MG_TOOLCHAIN_ROOT}")

# Configure how the find_* commands search
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY BOTH)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE BOTH)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE BOTH)

set(MG_TOOLCHAIN_VERSION 7)

# Set compiler
set(CMAKE_C_COMPILER   "${MG_TOOLCHAIN_ROOT}/bin/gcc"   CACHE STRING "" FORCE)
set(CMAKE_CXX_COMPILER "${MG_TOOLCHAIN_ROOT}/bin/g++" CACHE STRING "" FORCE)

set(CMAKE_CXX_COMPILER_AR "${MG_TOOLCHAIN_ROOT}/bin/gcc-ar" CACHE FILEPATH "C++ compiler archiver" FORCE)
set(CMAKE_CXX_COMPILER_RANLIB "${MG_TOOLCHAIN_ROOT}/bin/gcc-ranlib" CACHE FILEPATH "C++ compiler ranlib" FORCE)
set(CMAKE_C_COMPILER_AR "${MG_TOOLCHAIN_ROOT}/bin/gcc-ar" CACHE FILEPATH "C compiler archiver" FORCE)
set(CMAKE_C_COMPILER_RANLIB "${MG_TOOLCHAIN_ROOT}/bin/gcc-ranlib" CACHE FILEPATH "C compiler ranlib" FORCE)
set(CMAKE_AR "${MG_TOOLCHAIN_ROOT}/bin/gcc-ar" CACHE FILEPATH "Archiver" FORCE)
set(CMAKE_RANLIB "${MG_TOOLCHAIN_ROOT}/bin/gcc-ranlib" CACHE FILEPATH "Ranlib" FORCE)

# Linker
set(CMAKE_LINKER_TYPE DEFAULT)
set(CMAKE_LINKER "${MG_TOOLCHAIN_ROOT}/bin/ld")

# NM (symbol listing)
set(CMAKE_NM "${MG_TOOLCHAIN_ROOT}/bin/nm")

# Objcopy
set(CMAKE_OBJCOPY "${MG_TOOLCHAIN_ROOT}/bin/objcopy")

# Objdump
set(CMAKE_OBJDUMP "${MG_TOOLCHAIN_ROOT}/bin/objdump")

# Strip
set(CMAKE_STRIP "${MG_TOOLCHAIN_ROOT}/bin/strip")

# clang-scan-deps
#set(CMAKE_CXX_COMPILER_CLANG_SCAN_DEPS "${MG_TOOLCHAIN_ROOT}/bin/clang-scan-deps" CACHE STRING "" FORCE)

# Add toolchain to prefix path
list(APPEND CMAKE_PREFIX_PATH "${MG_TOOLCHAIN_ROOT}")
