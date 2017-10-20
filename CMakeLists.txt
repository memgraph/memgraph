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
# NOTE: must be choosen before use of project() or enable_language() ----------
set(CMAKE_C_COMPILER "clang")
set(CMAKE_CXX_COMPILER "clang++")
# -----------------------------------------------------------------------------

project(memgraph VERSION 0.8.0)
# -----------------------------------------------------------------------------

# setup CMake module path, defines path for include() and find_package()
# https://cmake.org/cmake/help/latest/variable/CMAKE_MODULE_PATH.html
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PROJECT_SOURCE_DIR}/cmake)
# -----------------------------------------------------------------------------

# custom function definitions
include(functions)
# -----------------------------------------------------------------------------

# We want out of source builds, so that cmake generated files don't get mixed
# with source files. This allows for easier clean up.
disallow_in_source_build()
add_custom_target(clean_all
                  COMMAND ${CMAKE_COMMAND} -P ${PROJECT_SOURCE_DIR}/cmake/clean_all.cmake
                  COMMENT "Removing all files in ${CMAKE_BINARY_DIR}")

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

# -----------------------------------------------------------------------------

# TODO: set here 17 once it will be available in the cmake version (3.8)
# set(CMAKE_CXX_STANDARD 17)
# set(CMAKE_CXX_STANDARD_REQUIRED ON)
# For now, explicitly set -std= flag for C++17.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1z -Wall")

# Don't omit frame pointer in RelWithDebInfo, for additional callchain debug.
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO
    "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fno-omit-frame-pointer")

# Use gold linker to speedup build
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=gold")
# -----------------------------------------------------------------------------

# dir variables
set(src_dir ${CMAKE_SOURCE_DIR}/src)
set(libs_dir ${CMAKE_SOURCE_DIR}/libs)
set(tests_dir ${CMAKE_SOURCE_DIR}/tests)
# -----------------------------------------------------------------------------

# Generate a version.hpp file
set(VERSION_STRING ${memgraph_VERSION})
configure_file(${src_dir}/version.hpp.in include/version.hpp @ONLY)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/include)

# build flags -----------------------------------------------------------------
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

# default build type is debug
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()
message(STATUS "CMake build type: ${CMAKE_BUILD_TYPE}")
# -----------------------------------------------------------------------------

# setup external dependencies -------------------------------------------------
add_subdirectory(libs)

# ndebug
option(NDEBUG "No debug" OFF)
message(STATUS "NDEBUG: ${NDEBUG} (be careful CMAKE_BUILD_TYPE can also \
append this flag)")
if(NDEBUG)
    add_definitions( -DNDEBUG )
endif()
# -----------------------------------------------------------------------------

# proof of concept
option(POC "Build proof of concept binaries" ON)
message(STATUS "POC binaries: ${POC}")
# experimental
option(EXPERIMENTAL "Build experimental binaries" OFF)
message(STATUS "POC binaries: ${POC}")
# tests
option(TEST_COVERAGE "Generate coverage reports from unit tests" OFF)
message(STATUS "Generate coverage from unit tests: ${TEST_COVERAGE}")
# -----------------------------------------------------------------------------

# includes
include_directories(${src_dir})
include_directories(SYSTEM ${GFLAGS_INCLUDE_DIR})
include_directories(SYSTEM ${GLOG_INCLUDE_DIR})
include_directories(SYSTEM ${FMT_INCLUDE_DIR})
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

# include antlr header files
include_directories(${ANTLR4_INCLUDE_DIR})

add_library(antlr_opencypher_parser_lib STATIC ${antlr_opencypher_generated_src})
target_link_libraries(antlr_opencypher_parser_lib antlr4)
# -----------------------------------------------------------------------------

# all memgraph src files
set(memgraph_src_files
    ${src_dir}/communication/bolt/v1/decoder/decoded_value.cpp
    ${src_dir}/communication/bolt/v1/session.cpp
    ${src_dir}/data_structures/concurrent/skiplist_gc.cpp
    ${src_dir}/database/graph_db.cpp
    ${src_dir}/database/graph_db_accessor.cpp
    ${src_dir}/database/dbms.cpp
    ${src_dir}/durability/recovery.cpp
    ${src_dir}/durability/snapshooter.cpp
    ${src_dir}/io/network/addrinfo.cpp
    ${src_dir}/io/network/network_endpoint.cpp
    ${src_dir}/io/network/socket.cpp
    ${src_dir}/query/common.cpp
    ${src_dir}/query/console.cpp
    ${src_dir}/query/frontend/ast/ast.cpp
    ${src_dir}/query/frontend/ast/cypher_main_visitor.cpp
    ${src_dir}/query/frontend/semantic/symbol_generator.cpp
    ${src_dir}/query/frontend/stripped.cpp
    ${src_dir}/query/interpret/awesome_memgraph_functions.cpp
    ${src_dir}/query/interpreter.cpp
    ${src_dir}/query/plan/operator.cpp
    ${src_dir}/query/plan/rule_based_planner.cpp
    ${src_dir}/query/plan/variable_start_planner.cpp
    ${src_dir}/query/typed_value.cpp
    ${src_dir}/storage/edge_accessor.cpp
    ${src_dir}/storage/locking/record_lock.cpp
    ${src_dir}/storage/property_value.cpp
    ${src_dir}/storage/record_accessor.cpp
    ${src_dir}/storage/vertex_accessor.cpp
    ${src_dir}/threading/thread.cpp
    ${src_dir}/transactions/transaction.cpp
)
# -----------------------------------------------------------------------------

# memgraph_lib depend on these libraries
set(MEMGRAPH_ALL_LIBS stdc++fs Threads::Threads fmt cppitertools
    antlr_opencypher_parser_lib dl glog gflags)

if (USE_LTALLOC)
    list(APPEND MEMGRAPH_ALL_LIBS ltalloc)
# TODO(mferencevic): Enable this when clang is updated on apollo.
#    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -flto")
endif()

if (READLINE_FOUND)
    list(APPEND MEMGRAPH_ALL_LIBS readline)
endif()

# STATIC library used by memgraph executables
add_library(memgraph_lib STATIC ${memgraph_src_files})
target_link_libraries(memgraph_lib ${MEMGRAPH_ALL_LIBS})
add_dependencies(memgraph_lib generate_opencypher_parser)
# -----------------------------------------------------------------------------

# proof of concepts
if (POC)
    add_subdirectory(poc)
endif()

# experimental
if (EXPERIMENTAL)
    add_subdirectory(experimental)
endif()
# -----------------------------------------------------------------------------

# tests
enable_testing()
add_subdirectory(tests)
# -----------------------------------------------------------------------------

# memgraph build name
execute_process(
    OUTPUT_VARIABLE COMMIT_HASH
    COMMAND git rev-parse --short HEAD
)
string(STRIP ${COMMIT_HASH} COMMIT_HASH)
set(MEMGRAPH_BUILD_NAME "memgraph-${memgraph_VERSION}-${COMMIT_HASH}_${CMAKE_BUILD_TYPE}")
add_custom_target(memgraph_link_target ALL
  COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_BINARY_DIR}/${MEMGRAPH_BUILD_NAME} ${CMAKE_BINARY_DIR}/memgraph DEPENDS ${MEMGRAPH_BUILD_NAME})
# -----------------------------------------------------------------------------

# memgraph main executable
add_executable(${MEMGRAPH_BUILD_NAME} ${src_dir}/memgraph_bolt.cpp)
target_link_libraries(${MEMGRAPH_BUILD_NAME} memgraph_lib)
# Strip the executable in release build.
string(TOLOWER ${CMAKE_BUILD_TYPE} lower_build_type)
if (lower_build_type STREQUAL "release")
  add_custom_command(TARGET ${MEMGRAPH_BUILD_NAME} POST_BUILD
                     COMMAND strip -s ${MEMGRAPH_BUILD_NAME}
                     COMMENT Stripping symbols and sections from
                             ${MEMGRAPH_BUILD_NAME})
endif()
# Install and rename executable to just 'memgraph' Since we have to rename,
# we cannot use the recommended `install(TARGETS ...)`.
install(PROGRAMS ${CMAKE_BINARY_DIR}/${MEMGRAPH_BUILD_NAME}
        DESTINATION bin RENAME memgraph)
# Install the config file.
install(FILES ${CMAKE_SOURCE_DIR}/config/community.conf
        DESTINATION /etc/memgraph RENAME memgraph.conf)
# Create empty directories for default location of snapshots and logs.
install(CODE "file(MAKE_DIRECTORY \$ENV{DESTDIR}/var/log/memgraph
                   \$ENV{DESTDIR}/var/lib/memgraph/snapshots)")

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
# TODO: Longer description, readme and license files.
# DEB specific
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Memgraph Ltd.")
set(CPACK_DEBIAN_PACKAGE_SECTION database)
set(CPACK_DEBIAN_PACKAGE_HOMEPAGE https://memgraph.com)
# We may need conffiles, postinst, postrm or prerm scripts.
# set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA ...)
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
# All variables must be set before including.
include(CPack)
# ---- End Setup CPack ----
