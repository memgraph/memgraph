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
    set(MG_TOOLCHAIN_ROOT "/opt/toolchain-v8")
endif()
message(STATUS "Toolchain directory: ${MG_TOOLCHAIN_ROOT}")

# Sysroot: everything the toolchain compiles (conan deps + memgraph itself)
# must target the bundled glibc 2.31, not the host's. Without --sysroot,
# clang picks up /usr/include and /usr/lib from the build host and the
# resulting binaries carry host-glibc symbol versions (GLIBC_2.34, 2.39, ...).
set(MG_TOOLCHAIN_SYSROOT "${MG_TOOLCHAIN_ROOT}/sysroot")
set(CMAKE_SYSROOT "${MG_TOOLCHAIN_SYSROOT}")

# Paths for find_package(), find_library(), etc. — list sysroot first.
set(CMAKE_FIND_ROOT_PATH "${MG_TOOLCHAIN_SYSROOT};${MG_TOOLCHAIN_ROOT};${CMAKE_FIND_ROOT_PATH}")

# find_* modes:
#   PROGRAM = NEVER  → use host build tools (git, make, ...).
#   LIBRARY/INCLUDE/PACKAGE = BOTH  → ONLY would re-root every absolute path
#     in CMAKE_PREFIX_PATH / CMAKE_LIBRARY_PATH / CMAKE_INCLUDE_PATH (set by
#     conan to point at ~/.conan2/p/...) under the sysroot, where they don't
#     exist; conan deps would silently fail to resolve. BOTH keeps conan's
#     prefix paths working while still letting the sysroot's libs be found
#     first via CMAKE_FIND_ROOT_PATH ordering.
#
# Compile / link isolation from the host (so we don't pick up host-glibc
# symbols) is handled by --sysroot=$SYSROOT, baked into CMAKE_SYSROOT above.
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY BOTH)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE BOTH)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE BOTH)

set(MG_TOOLCHAIN_VERSION 8)

# Set compiler
set(CMAKE_C_COMPILER   "${MG_TOOLCHAIN_ROOT}/bin/clang"   CACHE STRING "" FORCE)
set(CMAKE_CXX_COMPILER "${MG_TOOLCHAIN_ROOT}/bin/clang++" CACHE STRING "" FORCE)

# Set archiver and ranlib
set(CMAKE_CXX_COMPILER_AR "${MG_TOOLCHAIN_ROOT}/bin/llvm-ar" CACHE FILEPATH "C++ compiler archiver" FORCE)
set(CMAKE_CXX_COMPILER_RANLIB "${MG_TOOLCHAIN_ROOT}/bin/llvm-ranlib" CACHE FILEPATH "C++ compiler ranlib" FORCE)
set(CMAKE_C_COMPILER_AR "${MG_TOOLCHAIN_ROOT}/bin/llvm-ar" CACHE FILEPATH "C compiler archiver" FORCE)
set(CMAKE_C_COMPILER_RANLIB "${MG_TOOLCHAIN_ROOT}/bin/llvm-ranlib" CACHE FILEPATH "C compiler ranlib" FORCE)
set(CMAKE_AR "${MG_TOOLCHAIN_ROOT}/bin/llvm-ar" CACHE FILEPATH "Archiver" FORCE)
set(CMAKE_RANLIB "${MG_TOOLCHAIN_ROOT}/bin/llvm-ranlib" CACHE FILEPATH "Ranlib" FORCE)

# Linker
set(CMAKE_LINKER_TYPE LLD)
set(CMAKE_LINKER "${MG_TOOLCHAIN_ROOT}/bin/lld")

# NM (symbol listing)
set(CMAKE_NM "${MG_TOOLCHAIN_ROOT}/bin/llvm-nm")

# Objcopy
set(CMAKE_OBJCOPY "${MG_TOOLCHAIN_ROOT}/bin/llvm-objcopy")

# Objdump
set(CMAKE_OBJDUMP "${MG_TOOLCHAIN_ROOT}/bin/llvm-objdump")

# Strip
set(CMAKE_STRIP "${MG_TOOLCHAIN_ROOT}/bin/llvm-strip")

# clang-scan-deps
set(CMAKE_CXX_COMPILER_CLANG_SCAN_DEPS "${MG_TOOLCHAIN_ROOT}/bin/clang-scan-deps" CACHE STRING "" FORCE)

# Add toolchain to prefix path
list(APPEND CMAKE_PREFIX_PATH "${MG_TOOLCHAIN_ROOT}")

# Exclude OpenSSL from toolchain and sysroot search paths so conan-provided
# OpenSSL is used. The sysroot ships its own OpenSSLConfig.cmake (installed
# when openssl was built into the sysroot) but it only defines OpenSSL::SSL /
# OpenSSL::Crypto, not the openssl::openssl aggregate target that some conan
# recipes (e.g. nuraft) expect.
list(APPEND CMAKE_IGNORE_PATH "${MG_TOOLCHAIN_ROOT}/lib/cmake/OpenSSL")
list(APPEND CMAKE_IGNORE_PATH "${MG_TOOLCHAIN_ROOT}/lib64/cmake/OpenSSL")
list(APPEND CMAKE_IGNORE_PATH "${MG_TOOLCHAIN_SYSROOT}/usr/lib/cmake/OpenSSL")
list(APPEND CMAKE_IGNORE_PATH "${MG_TOOLCHAIN_SYSROOT}/usr/lib64/cmake/OpenSSL")
