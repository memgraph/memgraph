# MemGraph CMake configuration

cmake_minimum_required(VERSION 3.1)

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
# NOTE: must be choosen before use of project() or enable_language()
find_program(CLANG_FOUND clang)
find_program(CLANGXX_FOUND clang++)
if (CLANG_FOUND AND CLANGXX_FOUND)
  set(CMAKE_C_COMPILER ${CLANG_FOUND})
  set(CMAKE_CXX_COMPILER ${CLANGXX_FOUND})
endif()

# Get current commit hash.
execute_process(
    OUTPUT_VARIABLE COMMIT_HASH
    COMMAND git rev-parse --short HEAD
)
string(STRIP ${COMMIT_HASH} COMMIT_HASH)

# -----------------------------------------------------------------------------

project(memgraph VERSION 0.11.0)
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

# TODO: set here 17 once it will be available in the cmake version (3.8)
# set(CMAKE_CXX_STANDARD 17)
# set(CMAKE_CXX_STANDARD_REQUIRED ON)
# For now, explicitly set -std= flag for C++17.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1z -Wall \
    -Werror=switch -Werror=switch-bool -Werror=return-type")

# Don't omit frame pointer in RelWithDebInfo, for additional callchain debug.
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO
    "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fno-omit-frame-pointer")

# Statically link libgcc and libstdc++, the GCC allows this according to:
# https://www.gnu.org/licenses/gcc-exception-faq.html
# Last checked for gcc-7.3, we are using gcc-6.3 on build machines (license is
# valid there also).
# ** If we change versions, recheck this! **
# ** Static linking is allowed only for executables! **
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++")

# Use gold linker to speedup build
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=gold")

# release flags
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")

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

# ndebug
option(NDEBUG "No debug" OFF)
message(STATUS "NDEBUG: ${NDEBUG} (be careful CMAKE_BUILD_TYPE can also \
append this flag)")
if(NDEBUG)
    add_definitions( -DNDEBUG )
endif()
# -----------------------------------------------------------------------------

# default build type is debug
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()
message(STATUS "CMake build type: ${CMAKE_BUILD_TYPE}")
# -----------------------------------------------------------------------------

# setup external dependencies -------------------------------------------------

# threading
find_package(Threads REQUIRED)

# optional Ltalloc
option(USE_LTALLOC "Use Ltalloc instead of default allocator (default OFF). \
Set this to ON to link with Ltalloc." OFF)

# optional readline
option(USE_READLINE "Use GNU Readline library if available (default ON). \
Set this to OFF to prevent linking with Readline even if it is available." ON)
if (USE_READLINE)
  find_package(Readline)
  if (READLINE_FOUND)
    add_definitions(-DHAS_READLINE)
  endif()
endif()

set(Boost_USE_STATIC_LIBS ON)
find_package(Boost 1.62 REQUIRED COMPONENTS iostreams serialization)

# OpenSSL
find_package(OpenSSL REQUIRED)

set(libs_dir ${CMAKE_SOURCE_DIR}/libs)
add_subdirectory(libs EXCLUDE_FROM_ALL)

include_directories(SYSTEM ${GFLAGS_INCLUDE_DIR})
include_directories(SYSTEM ${GLOG_INCLUDE_DIR})
include_directories(SYSTEM ${FMT_INCLUDE_DIR})
include_directories(SYSTEM ${ANTLR4_INCLUDE_DIR})
include_directories(SYSTEM ${BZIP2_INCLUDE_DIR})
include_directories(SYSTEM ${ZLIB_INCLUDE_DIR})
include_directories(SYSTEM ${ROCKSDB_INCLUDE_DIR})
include_directories(SYSTEM ${CAPNP_INCLUDE_DIR})
# -----------------------------------------------------------------------------

# openCypher parser -----------------------------------------------------------
set(opencypher_frontend ${CMAKE_SOURCE_DIR}/src/query/frontend/opencypher)
set(opencypher_generated ${opencypher_frontend}/generated)
set(opencypher_grammar ${opencypher_frontend}/grammar/Cypher.g4)

# enumerate all files that are generated from antlr
set(antlr_opencypher_generated_src
   ${opencypher_generated}/CypherLexer.cpp
   ${opencypher_generated}/CypherParser.cpp
   ${opencypher_generated}/CypherBaseVisitor.cpp
   ${opencypher_generated}/CypherVisitor.cpp
)

# Provide a command to generate sources if missing. If this were a
# custom_target, it would always run and we don't want that.
add_custom_command(OUTPUT ${antlr_opencypher_generated_src}
  COMMAND
  ${CMAKE_COMMAND} -E make_directory ${opencypher_generated}
  COMMAND
  java -jar ${CMAKE_SOURCE_DIR}/libs/antlr-4.6-complete.jar -Dlanguage=Cpp -visitor -o ${opencypher_generated} -package antlropencypher ${opencypher_grammar}
  WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
  DEPENDS ${opencypher_grammar})

# add custom target for generation
add_custom_target(generate_opencypher_parser
  DEPENDS ${antlr_opencypher_generated_src})

add_library(antlr_opencypher_parser_lib STATIC ${antlr_opencypher_generated_src})
target_link_libraries(antlr_opencypher_parser_lib antlr4)
# -----------------------------------------------------------------------------

# Optional subproject configuration -------------------------------------------
option(POC "Build proof of concept binaries" OFF)
option(EXPERIMENTAL "Build experimental binaries" OFF)
option(CUSTOMERS "Build customer binaries" OFF)
option(TEST_COVERAGE "Generate coverage reports from running memgraph" OFF)
option(TOOLS "Build tools binaries" ON)
option(MG_COMMUNITY "Build Memgraph Community Edition" OFF)
option(ASAN "Build with Address Sanitizer. To get a reasonable performance option should be used only in Release or RelWithDebInfo build " OFF)
option(TSAN "Build with Thread Sanitizer. To get a reasonable performance option should be used only in Release or RelWithDebInfo build " OFF)
option(UBSAN "Build with Undefined Behaviour Sanitizer" OFF)
option(THIN_LTO "Build with link time optimization" OFF)

if (TEST_COVERAGE)
  string(TOLOWER ${CMAKE_BUILD_TYPE} lower_build_type)
  if (NOT lower_build_type STREQUAL "debug")
    message(FATAL_ERROR "Generating test coverage unsupported in non Debug builds. Current build type is '${CMAKE_BUILD_TYPE}'")
  endif()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
endif()

if (MG_COMMUNITY)
  add_definitions(-DMG_COMMUNITY)
endif()

if (ASAN)
  # Enable Addres sanitizer and get nicer stack traces in error messages.
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
  # Run program with environment variable UBSAN_OPTIONS=print_stacktrace=1
  # Make sure llvm-symbolizer binary is in path
endif()

if (THIN_LTO)
  set(CMAKE_CXX_FLAGS"${CMAKE_CXX_FLAGS} -flto=thin")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -flto=thin")
endif()

# Add subprojects
include_directories(src)
add_subdirectory(src)

if(POC)
  add_subdirectory(poc)
endif()

if(EXPERIMENTAL)
  add_subdirectory(experimental)
endif()

if(CUSTOMERS)
  add_subdirectory(customers)
endif()

enable_testing()
add_subdirectory(tests)

if(TOOLS)
  add_subdirectory(tools)
endif()

# -----------------------------------------------------------------------------

# ---- Setup CPack --------
# General setup
set(CPACK_PACKAGE_NAME memgraph)
set(CPACK_PACKAGE_VENDOR "Memgraph Ltd.")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY
    "High performance, in-memory, transactional graph database")
set(CPACK_PACKAGE_VERSION_MAJOR ${memgraph_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${memgraph_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${memgraph_VERSION_PATCH})
set(CPACK_PACKAGE_VERSION_TWEAK ${memgraph_VERSION_TWEAK})
set(CPACK_PACKAGE_FILE_NAME ${CPACK_PACKAGE_NAME}-${memgraph_VERSION}-${COMMIT_HASH}${CPACK_SYSTEM_NAME})

# DEB specific
# Instead of using "name <email>" format, we use "email (name)" to prevent
# errors due to full stop, '.' at the end of "Ltd". (See: RFC 822)
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "tech@memgraph.com (Memgraph Ltd.)")
set(CPACK_DEBIAN_PACKAGE_SECTION non-free/database)
set(CPACK_DEBIAN_PACKAGE_HOMEPAGE https://memgraph.com)
set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA
    "${CMAKE_SOURCE_DIR}/release/debian/conffiles;"
    "${CMAKE_SOURCE_DIR}/release/debian/copyright;"
    "${CMAKE_SOURCE_DIR}/release/debian/prerm;"
    "${CMAKE_SOURCE_DIR}/release/debian/postrm;"
    "${CMAKE_SOURCE_DIR}/release/debian/postinst;")
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
# Description formatting is important, summary must be followed with a newline and 1 space.
set(CPACK_DEBIAN_PACKAGE_DESCRIPTION "${CPACK_PACKAGE_DESCRIPTION_SUMMARY}
 Contains Memgraph, the graph database. It aims to deliver developers the
 speed, simplicity and scale required to build the next generation of
 applications driver by real-time connected data.")
# Add `openssl` package to dependencies list. Used to generate SSL certificates.
set(CPACK_DEBIAN_memgraph_PACKAGE_DEPENDS "openssl (>= 1.1.0)")

# RPM specific
set(CPACK_RPM_PACKAGE_URL https://memgraph.com)
set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION
  /var /var/lib /var/log /etc/logrotate.d
  /lib /lib/systemd /lib/systemd/system /lib/systemd/system/memgraph.service)
set(CPACK_RPM_PACKAGE_REQUIRES_PRE "shadow-utils")
# NOTE: user specfile has a bug in cmake 3.7.2, this needs to be patched
# manually in: ~/cmake/share/cmake-3.7/Modules/CPackRPM.cmake line 2273
# Or newer cmake version used
set(CPACK_RPM_USER_BINARY_SPECFILE "${CMAKE_SOURCE_DIR}/release/rpm/memgraph.spec.in")
# Description formatting is important, no line must be greater than 80 characters.
set(CPACK_RPM_PACKAGE_DESCRIPTION "Contains Memgraph, the graph database.
It aims to deliver developers the speed, simplicity and scale required to build
the next generation of applications driver by real-time connected data.")
# Add `openssl` package to dependencies list. Used to generate SSL certificates.
set(CPACK_RPM_memgraph_PACKAGE_REQUIRES "openssl >= 1.0.0")

# All variables must be set before including.
include(CPack)
# ---- End Setup CPack ----
