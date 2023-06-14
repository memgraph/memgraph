# MemGraph CMake configuration

cmake_minimum_required(VERSION 3.8)

# !! IMPORTANT !! run ./project_root/init.sh before cmake command
# to download dependencies

if(NOT UNIX)
  message(FATAL_ERROR "Unsupported operating system.")
endif()

# Set `make clean` to ignore outputs of add_custom_command. If generated files
# need to be cleaned, set ADDITIONAL_MAKE_CLEAN_FILES property.
set_directory_properties(PROPERTIES CLEAN_NO_CUSTOM TRUE)

# ccache setup
# ccache isn't enabled all the time because it makes some problem
# during the code coverage process
find_program(CCACHE_FOUND ccache)
option(USE_CCACHE "ccache:" ON)
message(STATUS "CCache: ${USE_CCACHE}")
if(CCACHE_FOUND AND USE_CCACHE)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND AND USE_CCACHE)

# choose a compiler
# NOTE: must be chosen before use of project() or enable_language()
find_program(CLANG_FOUND clang)
find_program(CLANGXX_FOUND clang++)
if (CLANG_FOUND AND CLANGXX_FOUND)
  set(CMAKE_C_COMPILER ${CLANG_FOUND})
  set(CMAKE_CXX_COMPILER ${CLANGXX_FOUND})
else()
  message(FATAL_ERROR "Couldn't find clang and/or clang++!")
endif()

# -----------------------------------------------------------------------------

project(memgraph)

# Install licenses.
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/licenses/
        DESTINATION share/doc/memgraph)

# For more information about how to release a new version of Memgraph, see
# `release/README.md`.

# Option that is used to specify which version of Memgraph should be built. The
# default is `ON` which causes the build system to build Memgraph Enterprise.
# Memgraph Community is built if explicitly set to `OFF`.
option(MG_ENTERPRISE "Build Memgraph Enterprise Edition" ON)

# Set the current version here to override the automatic version detection. The
# version must be specified as `X.Y.Z`. Primarily used when building new patch
# versions.
set(MEMGRAPH_OVERRIDE_VERSION "")

# Custom suffix that this version should have. The suffix can be any arbitrary
# string. Primarily used when building a version for a specific customer.
set(MEMGRAPH_OVERRIDE_VERSION_SUFFIX "")

# Variables used to generate the versions.
if (MG_ENTERPRISE)
  set(get_version_offering "")
else()
  set(get_version_offering "--open-source")
endif()
set(get_version_script "${CMAKE_CURRENT_SOURCE_DIR}/release/get_version.py")

# Get version that should be used in the binary.
execute_process(
    OUTPUT_VARIABLE MEMGRAPH_VERSION
    RESULT_VARIABLE MEMGRAPH_VERSION_RESULT
    COMMAND "${get_version_script}" ${get_version_offering}
                "${MEMGRAPH_OVERRIDE_VERSION}"
                "${MEMGRAPH_OVERRIDE_VERSION_SUFFIX}"
                "--memgraph-root-dir"
                "${CMAKE_CURRENT_SOURCE_DIR}"
)
if(MEMGRAPH_VERSION_RESULT AND NOT MEMGRAPH_VERSION_RESULT EQUAL 0)
    message(FATAL_ERROR "Unable to get Memgraph version.")
else()
    MESSAGE(STATUS "Memgraph version: ${MEMGRAPH_VERSION}")
endif()

# Get version that should be used in the DEB package.
execute_process(
    OUTPUT_VARIABLE MEMGRAPH_VERSION_DEB
    RESULT_VARIABLE MEMGRAPH_VERSION_DEB_RESULT
    COMMAND "${get_version_script}" ${get_version_offering}
                --variant deb
                "${MEMGRAPH_OVERRIDE_VERSION}"
                "${MEMGRAPH_OVERRIDE_VERSION_SUFFIX}"
                "--memgraph-root-dir"
                "${CMAKE_CURRENT_SOURCE_DIR}"
)
if(MEMGRAPH_VERSION_DEB_RESULT AND NOT MEMGRAPH_VERSION_DEB_RESULT EQUAL 0)
    message(FATAL_ERROR "Unable to get Memgraph DEB version.")
else()
    MESSAGE(STATUS "Memgraph DEB version: ${MEMGRAPH_VERSION_DEB}")
endif()

# Get version that should be used in the RPM package.
execute_process(
    OUTPUT_VARIABLE MEMGRAPH_VERSION_RPM
    RESULT_VARIABLE MEMGRAPH_VERSION_RPM_RESULT
    COMMAND "${get_version_script}" ${get_version_offering}
                --variant rpm
                "${MEMGRAPH_OVERRIDE_VERSION}"
                "${MEMGRAPH_OVERRIDE_VERSION_SUFFIX}"
                "--memgraph-root-dir"
                "${CMAKE_CURRENT_SOURCE_DIR}"
)
if(MEMGRAPH_VERSION_RPM_RESULT AND NOT MEMGRAPH_VERSION_RPM_RESULT EQUAL 0)
    message(FATAL_ERROR "Unable to get Memgraph RPM version.")
else()
    MESSAGE(STATUS "Memgraph RPM version: ${MEMGRAPH_VERSION_RPM}")
endif()

# We want the above variables to be updated each time something is committed to
# the repository. That is why we include a dependency on the current git HEAD
# to trigger a new CMake run when the git repository state changes. This is a
# hack, as CMake doesn't have a mechanism to regenerate variables when
# something changes (only files can be regenerated).
# https://cmake.org/pipermail/cmake/2018-October/068389.html
#
# The hack in the above link is nearly correct but it has a fatal flaw. The
# `CMAKE_CONFIGURE_DEPENDS` isn't a `GLOBAL` property, it is instead a
# `DIRECTORY` property and as such must be set in the `DIRECTORY` scope.
# https://cmake.org/cmake/help/v3.14/manual/cmake-properties.7.html
#
# Unlike the above mentioned hack, we don't use the `.git/index` file. That
# file changes on every `git add` (even on `git status`) so it triggers
# unnecessary recalculations of the release version. The release version only
# changes on every `git commit` or `git checkout`. That is why we watch the
# following files for changes:
#   - `.git/HEAD` -> changes each time a `git checkout` is issued
#   - `.git/refs/heads/...` -> the value in `.git/HEAD` is a branch name (when
#     you are on a branch) and you have to monitor the file of the specific
#     branch to detect when a `git commit` was issued
# More details about the contents of the `.git` directory and the specific
# files used can be seen here:
# https://git-scm.com/book/en/v2/Git-Internals-Git-References
set(git_directory "${CMAKE_SOURCE_DIR}/.git")
if (EXISTS "${git_directory}")
  set_property(DIRECTORY APPEND PROPERTY
      CMAKE_CONFIGURE_DEPENDS "${git_directory}/HEAD")
  file(STRINGS "${git_directory}/HEAD" git_head_data)
  if (git_head_data MATCHES "^ref: ")
    string(SUBSTRING "${git_head_data}" 5 -1 git_head_ref)
    set_property(DIRECTORY APPEND PROPERTY
        CMAKE_CONFIGURE_DEPENDS "${git_directory}/${git_head_ref}")
  endif()
endif()

# -----------------------------------------------------------------------------

# setup CMake module path, defines path for include() and find_package()
# https://cmake.org/cmake/help/latest/variable/CMAKE_MODULE_PATH.html
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PROJECT_SOURCE_DIR}/cmake)
# custom function definitions
include(functions)
# -----------------------------------------------------------------------------

# We want out of source builds, so that cmake generated files don't get mixed
# with source files. This allows for easier clean up.
disallow_in_source_build()
add_custom_target(clean_all
                  COMMAND ${CMAKE_COMMAND} -P ${PROJECT_SOURCE_DIR}/cmake/clean_all.cmake
                  COMMENT "Removing all files in ${CMAKE_BINARY_DIR}")
# -----------------------------------------------------------------------------

# build flags -----------------------------------------------------------------

# Export the compile commands so that we can use clang-tidy. Additional benefit
# is easier debugging of compilation and linker flags.
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# c99-designator is disabled because of required mixture of designated and
# non-designated initializers in Python Query Module code (`py_module.cpp`).
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall \
    -Werror=switch -Werror=switch-bool -Werror=return-type \
    -Werror=return-stack-address \
    -Wno-c99-designator \
    -DBOOST_ASIO_USE_TS_EXECUTOR_AS_DEFAULT")

# Don't omit frame pointer in RelWithDebInfo, for additional callchain debug.
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO
    "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fno-omit-frame-pointer")

# Statically link libgcc and libstdc++, the GCC allows this according to:
# https://gcc.gnu.org/onlinedocs/gcc-10.2.0/libstdc++/manual/manual/license.html
# https://www.gnu.org/licenses/gcc-exception-faq.html
# Last checked for gcc-10.2 which we are using on the build machines.
# ** If we change versions, recheck this! **
# ** Static linking is allowed only for executables! **
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++")

# Use gold linker to speedup build
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=gold")

# release flags
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")

SET(CMAKE_CXX_LINK_FLAGS "${CMAKE_CXX_LINK_FLAGS} -pthread")

#debug flags
set(PREFERRED_DEBUGGER "gdb" CACHE STRING
    "Tunes the debug output for your preferred debugger (gdb or lldb).")
if ("${PREFERRED_DEBUGGER}" STREQUAL "gdb" AND
    "${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang|GNU")
    set(CMAKE_CXX_FLAGS_DEBUG "-ggdb")
elseif ("${PREFERRED_DEBUGGER}" STREQUAL "lldb" AND
        "${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS_DEBUG "-glldb")
else()
    message(WARNING "Unable to tune for PREFERRED_DEBUGGER: "
            "'${PREFERRED_DEBUGGER}' with compiler: '${CMAKE_CXX_COMPILER_ID}'")
    set(CMAKE_CXX_FLAGS_DEBUG "-g")
endif()

# -----------------------------------------------------------------------------

# default build type is debug
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()
message(STATUS "CMake build type: ${CMAKE_BUILD_TYPE}")
# -----------------------------------------------------------------------------

add_definitions( -DCMAKE_BUILD_TYPE_NAME="${CMAKE_BUILD_TYPE}")

if (NOT MG_ARCH)
    set(MG_ARCH_DESCR "Host architecture to build Memgraph on. Supported values are x86_64, ARM64.")
    if (${CMAKE_HOST_SYSTEM_PROCESSOR} MATCHES "aarch64")
        set(MG_ARCH "ARM64" CACHE STRING ${MG_ARCH_DESCR})
    else()
        set(MG_ARCH "x86_64" CACHE STRING ${MG_ARCH_DESCR})
    endif()
endif()
message(STATUS "MG_ARCH: ${MG_ARCH}")

# setup external dependencies -------------------------------------------------

# threading
find_package(Threads REQUIRED)
# optional readline
option(USE_READLINE "Use GNU Readline library if available (default ON). \
Set this to OFF to prevent linking with Readline even if it is available." ON)
if (USE_READLINE)
  find_package(Readline)
  if (READLINE_FOUND)
    add_definitions(-DHAS_READLINE)
  endif()
endif()

set(libs_dir ${CMAKE_SOURCE_DIR}/libs)
add_subdirectory(libs EXCLUDE_FROM_ALL)

# Optional subproject configuration -------------------------------------------
option(TEST_COVERAGE "Generate coverage reports from running memgraph" OFF)
option(TOOLS "Build tools binaries" ON)
option(QUERY_MODULES "Build query modules containing custom procedures" ON)
option(ASAN "Build with Address Sanitizer. To get a reasonable performance option should be used only in Release or RelWithDebInfo build " OFF)
option(TSAN "Build with Thread Sanitizer. To get a reasonable performance option should be used only in Release or RelWithDebInfo build " OFF)
option(UBSAN "Build with Undefined Behaviour Sanitizer" OFF)

if (TEST_COVERAGE)
  string(TOLOWER ${CMAKE_BUILD_TYPE} lower_build_type)
  if (NOT lower_build_type STREQUAL "debug")
    message(FATAL_ERROR "Generating test coverage unsupported in non Debug builds. Current build type is '${CMAKE_BUILD_TYPE}'")
  endif()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
endif()

if (MG_ENTERPRISE)
  add_definitions(-DMG_ENTERPRISE)
endif()

set(ENABLE_JEMALLOC ON)

if (ASAN)
  message(WARNING "Disabling jemalloc as it doesn't work well with ASAN")
  set(ENABLE_JEMALLOC OFF)
  # Enable Address sanitizer and get nicer stack traces in error messages.
  # NOTE: AddressSanitizer uses llvm-symbolizer binary from the Clang
  # distribution to symbolize the stack traces (note that ideally the
  # llvm-symbolizer version must match the version of ASan runtime library).
  # Just make sure llvm-symbolizer is in PATH before running the binary or
  # provide it in separate ASAN_SYMBOLIZER_PATH environment variable.
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
  # To detect Stack-use-after-return bugs set run-time flag:
  #   ASAN_OPTIONS=detect_stack_use_after_return=1
  # To check initialization order bugs set run-time flag:
  #   ASAN_OPTIONS=check_initialization_order=true
  #     This mode reports an error if initializer for a global variable accesses
  #     dynamically initialized global from another translation unit, which is
  #     not yet initialized
  #   ASAN_OPTIONS=strict_init_order=true
  #     This mode reports an error if initializer for a global variable accesses
  #     any dynamically initialized global from another translation unit.
endif()

if (TSAN)
  # ThreadSanitizer generally requires all code to be compiled with -fsanitize=thread.
  # If some code (e.g. dynamic libraries) is not compiled with the flag, it can
  # lead to false positive race reports, false negative race reports and/or
  # missed stack frames in reports depending on the nature of non-instrumented
  # code. To not produce false positive reports ThreadSanitizer has to see all
  # synchronization in the program, some synchronization operations (namely,
  # atomic operations and thread-safe static initialization) are intercepted
  # during compilation (and can only be intercepted during compilation).
  # ThreadSanitizer stack trace collection also relies on compiler instrumentation
  # (unwinding stack on each memory access is too expensive).
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
  # By default ThreadSanitizer uses addr2line utility to symbolize reports.
  # llvm-symbolizer is faster, consumes less memory and produces much better
  # reports. To use it set runtime flag:
  #   TSAN_OPTIONS="extern-symbolizer-path=~/llvm-symbolizer"
  # For more runtime flags see: https://github.com/google/sanitizers/wiki/ThreadSanitizerFlags
endif()

if (UBSAN)
  # Compile with UBSAN but disable vptr check. This is disabled because it
  # requires linking with clang++ to make sure C++ specific parts of the
  # runtime library and c++ standard libraries are present.
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined -fno-omit-frame-pointer -fno-sanitize=vptr")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=undefined -fno-sanitize=vptr")
  # Run program with environment variable UBSAN_OPTIONS=print_stacktrace=1.
  # Make sure llvm-symbolizer binary is in path.
  # To make the program abort on undefined behavior, use UBSAN_OPTIONS=halt_on_error=1.
endif()

set(MG_PYTHON_VERSION "" CACHE STRING "Specify the exact Python version used by the query modules")
set(MG_PYTHON_PATH "" CACHE STRING "Specify the exact Python path used by the query modules")

# Add subprojects
include_directories(src)
add_subdirectory(src)

# Release configuration
add_subdirectory(release)

option(MG_ENABLE_TESTING "Set this to OFF to disable building test binaries" ON)
message(STATUS "MG_ENABLE_TESTING: ${MG_ENABLE_TESTING}")

if (MG_ENABLE_TESTING)
  enable_testing()
  add_subdirectory(tests)
endif()

if(TOOLS)
  add_subdirectory(tools)
endif()

if(QUERY_MODULES)
  add_subdirectory(query_modules)
endif()

install(FILES ${CMAKE_BINARY_DIR}/bin/mgconsole
  PERMISSIONS OWNER_EXECUTE OWNER_READ OWNER_WRITE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  TYPE BIN)
