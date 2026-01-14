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

if (DEFINED ENV{MG_TOOLCHAIN_ROOT})
    set(MG_TOOLCHAIN_ROOT "$ENV{MG_TOOLCHAIN_ROOT}")
else()
    set(MG_TOOLCHAIN_ROOT "/opt/toolchain-v7")
endif()
message(STATUS "Toolchain directory: ${MG_TOOLCHAIN_ROOT}")

# Paths for find_package(), find_library(), etc.
set(CMAKE_FIND_ROOT_PATH "${CMAKE_FIND_ROOT_PATH};${MG_TOOLCHAIN_ROOT}")

# Configure how the find_* commands search
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY BOTH)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE BOTH)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE BOTH)

set(MG_TOOLCHAIN_VERSION 7)

# EXPERIMENTAL: Override to use system LLVM-21 instead of toolchain LLVM
# This allows testing with a different LLVM version while still using toolchain libraries
set(LLVM21_ROOT "/usr/lib/llvm-21")
if(EXISTS "${LLVM21_ROOT}/bin/clang")
    message(STATUS "Using system LLVM-21 from ${LLVM21_ROOT}")
    set(LLVM_BIN_DIR "${LLVM21_ROOT}/bin")
else()
    message(STATUS "System LLVM-21 not found, falling back to toolchain LLVM")
    set(LLVM_BIN_DIR "${MG_TOOLCHAIN_ROOT}/bin")
endif()

# Set compiler
set(CMAKE_C_COMPILER   "${LLVM_BIN_DIR}/clang"   CACHE STRING "" FORCE)
set(CMAKE_CXX_COMPILER "${LLVM_BIN_DIR}/clang++" CACHE STRING "" FORCE)

# Set archiver and ranlib
set(CMAKE_CXX_COMPILER_AR "${LLVM_BIN_DIR}/llvm-ar" CACHE FILEPATH "C++ compiler archiver" FORCE)
set(CMAKE_CXX_COMPILER_RANLIB "${LLVM_BIN_DIR}/llvm-ranlib" CACHE FILEPATH "C++ compiler ranlib" FORCE)
set(CMAKE_C_COMPILER_AR "${LLVM_BIN_DIR}/llvm-ar" CACHE FILEPATH "C compiler archiver" FORCE)
set(CMAKE_C_COMPILER_RANLIB "${LLVM_BIN_DIR}/llvm-ranlib" CACHE FILEPATH "C compiler ranlib" FORCE)
set(CMAKE_AR "${LLVM_BIN_DIR}/llvm-ar" CACHE FILEPATH "Archiver" FORCE)
set(CMAKE_RANLIB "${LLVM_BIN_DIR}/llvm-ranlib" CACHE FILEPATH "Ranlib" FORCE)

# Linker
set(CMAKE_LINKER_TYPE LLD)
set(CMAKE_LINKER "${LLVM_BIN_DIR}/lld")

# NM (symbol listing)
set(CMAKE_NM "${LLVM_BIN_DIR}/llvm-nm")

# Objcopy
set(CMAKE_OBJCOPY "${LLVM_BIN_DIR}/llvm-objcopy")

# Objdump
set(CMAKE_OBJDUMP "${LLVM_BIN_DIR}/llvm-objdump")

# Strip
set(CMAKE_STRIP "${LLVM_BIN_DIR}/llvm-strip")

# clang-scan-deps
if(EXISTS "${LLVM_BIN_DIR}/clang-scan-deps")
    set(CMAKE_CXX_COMPILER_CLANG_SCAN_DEPS "${LLVM_BIN_DIR}/clang-scan-deps" CACHE STRING "" FORCE)
else()
    set(CMAKE_CXX_COMPILER_CLANG_SCAN_DEPS "${MG_TOOLCHAIN_ROOT}/bin/clang-scan-deps" CACHE STRING "" FORCE)
endif()

# Add toolchain to prefix path
list(APPEND CMAKE_PREFIX_PATH "${MG_TOOLCHAIN_ROOT}")

# Exclude OpenSSL from toolchain search paths to force use of Conan-provided OpenSSL
list(APPEND CMAKE_IGNORE_PATH "${MG_TOOLCHAIN_ROOT}/lib/cmake/OpenSSL")
list(APPEND CMAKE_IGNORE_PATH "${MG_TOOLCHAIN_ROOT}/lib64/cmake/OpenSSL")
