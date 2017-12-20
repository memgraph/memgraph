# MemGraph CMake configuration

cmake_minimum_required(VERSION 3.1)

# !! IMPORTANT !! run ./project_root/init.sh before cmake command
# to download dependencies

if(NOT UNIX)
    message(FATAL "Unsupported operating system.")
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

project(memgraph VERSION 0.9.0)
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
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1z -Wall -Werror=switch -Werror=switch-bool")

# Don't omit frame pointer in RelWithDebInfo, for additional callchain debug.
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO
    "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fno-omit-frame-pointer")

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
option(USE_LTALLOC "Use Ltalloc instead of default allocator (default ON). \
Set this to OFF to prevent linking with Ltalloc." ON)

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
find_package(Boost 1.62 REQUIRED COMPONENTS serialization)

set(libs_dir ${CMAKE_SOURCE_DIR}/libs)
add_subdirectory(libs EXCLUDE_FROM_ALL)

include_directories(SYSTEM ${GFLAGS_INCLUDE_DIR})
include_directories(SYSTEM ${GLOG_INCLUDE_DIR})
include_directories(SYSTEM ${FMT_INCLUDE_DIR})
include_directories(SYSTEM ${ANTLR4_INCLUDE_DIR})
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

include_directories(src)
add_subdirectory(src)
# -----------------------------------------------------------------------------

# Optional subproject configuration -------------------------------------------
option(POC "Build proof of concept binaries" ON)
option(EXPERIMENTAL "Build experimental binaries" OFF)
option(CUSTOMERS "Build customer binaries" ON)
option(TEST_COVERAGE "Generate coverage reports from unit tests" OFF)
option(TOOLS "Build tools binaries" OFF)

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
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/debian/conffiles;"
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/debian/copyright;"
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/debian/prerm;"
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/debian/postrm;"
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/debian/postinst;")
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
# Description formatting is important, summary must be followed with a newline and 1 space.
set(CPACK_DEBIAN_PACKAGE_DESCRIPTION "${CPACK_PACKAGE_DESCRIPTION_SUMMARY}
 Contains Memgraph, the graph database. It aims to deliver developers the
 speed, simplicity and scale required to build the next generation of
 applications driver by real-time connected data.")
# All variables must be set before including.
include(CPack)
# ---- End Setup CPack ----
