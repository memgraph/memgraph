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

# set project name
# get directory name
get_filename_component(project_name ${CMAKE_SOURCE_DIR} NAME)
# replace whitespaces with underscores
string(REPLACE " " "_" project_name ${project_name})
# set project name
project(${project_name})
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

# optional readline
find_package(Readline REQUIRED)
if (READLINE_FOUND)
  include_directories(SYSTEM ${READLINE_INCLUDE_DIR})
  add_definitions(-DHAS_READLINE)
endif()

# -----------------------------------------------------------------------------

# c++14
# TODO: set here 17 once it will be available in the cmake version (3.8)
set(cxx_standard 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1z -Wall -Wno-c++1z-extensions")
# Don't omit frame pointer in RelWithDebInfo, for additional callchain debug.
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO
    "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fno-omit-frame-pointer")
# -----------------------------------------------------------------------------

# dir variables
set(src_dir ${CMAKE_SOURCE_DIR}/src)
set(libs_dir ${CMAKE_SOURCE_DIR}/libs)
set(tests_dir ${CMAKE_SOURCE_DIR}/tests)
# -----------------------------------------------------------------------------

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
if ("${CMAKE_BUILD_TYPE}" STREQUAL "")
    set(CMAKE_BUILD_TYPE "Debug")
endif()
message(STATUS "CMake build type: ${CMAKE_BUILD_TYPE}")
# -----------------------------------------------------------------------------

# setup external dependencies -------------------------------------------------
add_subdirectory(libs)
# fmt
set(fmt_source_dir ${libs_dir}/fmt)
set(fmt_static_lib ${fmt_source_dir}/fmt/libfmt.a)

# linter setup (clang-tidy)
# all source files for linting
FILE(GLOB_RECURSE LINTER_SRC_FILES
    ${src_dir}/*.cpp
    ${CMAKE_SOURCE_DIR}/tests/.cpp
    ${CMAKE_SOURCE_DIR}/poc/.cpp
)
MESSAGE(STATUS "All cpp files for linting are: ${LINTER_SRC_FILES}")
# linter target clang-tidy
find_program(CLANG_TIDY "clang-tidy")
if(CLANG_TIDY)
    add_custom_target(
        clang-tidy
        COMMAND /usr/bin/clang-tidy
        ${LINTER_SRC_FILES}
        -config=''
        --
        -std=c++1y
        -I${CMAKE_SOURCE_DIR}/include -I${fmt_source_dir}
    )
endif()
# -----------------------------------------------------------------------------

# custom assert control parameters

# by default on custom assert only the message, filename and line number will be
# printed on stderr, if STACKTRACE_ASSERT is ON the whole stacktrace is going to
# be printed on stderr
option(STACKTRACE_ASSERT "Dump stacktrace on custom assert" OFF)
message(STATUS "STACKTRACE_ASSERT: ${STACKTRACE_ASSERT}")
if(STACKTRACE_ASSERT)
    add_definitions(-DSTACKTRACE_ASSERT_ON)
endif()
# -----------------------------------------------------------------------------

# ndebug
option(NDEBUG "No debug" OFF)
message(STATUS "NDEBUG: ${NDEBUG} (be careful CMAKE_BUILD_TYPE can also \
append this flag)")
if(NDEBUG)
    add_definitions( -DNDEBUG )
endif()
# -----------------------------------------------------------------------------

# -- GLIBCXX_DEBUG ------------------------------------------------------------
# glibcxx debug (useful for gdb)
# the problem is that the query engine doesn't work as it should work if
# this flag is present (TODO: figure out why)
option(GLIBCXX_DEBUG "glibc debug" OFF)
message(STATUS "GLIBCXX_DEBUG: ${GLIBCXX_DEBUG} (solves problem with \
_M_dataplus member during a debugging process)")
if(GLIBCXX_DEBUG)
    set(CMAKE_CXX_FLAGS_DEBUG "-D_GLIBCXX_DEBUG ${CMAKE_CXX_FLAGS_DEBUG}")
endif()
# -----------------------------------------------------------------------------

# option binaries
# memgraph
option(MEMGRAPH "Build memgraph binary" ON)
message(STATUS "MEMGRAPH binary: ${MEMGRAPH}")
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
include_directories(SYSTEM ${GTEST_INCLUDE_DIRS} ${GMOCK_INCLUDE_DIRS})
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/libs)  # cppitertools
# needed to include configured files (plan_compiler_flags.hpp)
set(generated_headers_dir ${CMAKE_BINARY_DIR}/generated_headers)
include_directories(${generated_headers_dir})
include_directories(SYSTEM ${GLOG_INCLUDE_DIR})
# -----------------------------------------------------------------------------

# openCypher parser -----------------------------------------------------------
set(antlr_src ${CMAKE_SOURCE_DIR}/libs/antlr4/runtime/Cpp/runtime/src)
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
include_directories(
  ${antlr_src}
  ${antlr_src}/misc
  ${antlr_src}/atn
  ${antlr_src}/dfa
  ${antlr_src}/tree
  ${antlr_src}/support
)

add_library(antlr_opencypher_parser_lib STATIC ${antlr_opencypher_generated_src})
target_link_libraries(antlr_opencypher_parser_lib antlr4_static)
# -----------------------------------------------------------------------------

# all memgraph src files
set(memgraph_src_files
    ${src_dir}/communication/bolt/v1/decoder/decoded_value.cpp
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

# memgraph_lib and memgraph_pic depend on these libraries
set(MEMGRAPH_ALL_LIBS stdc++fs Threads::Threads fmt
    antlr_opencypher_parser_lib dl ${GLOG_LIBRARY} gflags)
if (READLINE_FOUND)
    list(APPEND MEMGRAPH_ALL_LIBS ${READLINE_LIBRARY})
endif()

# STATIC library used by memgraph executables
add_library(memgraph_lib STATIC ${memgraph_src_files})
target_link_libraries(memgraph_lib ${MEMGRAPH_ALL_LIBS})
add_dependencies(memgraph_lib generate_opencypher_parser
                              glog)
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
add_subdirectory(tests)
# -----------------------------------------------------------------------------

add_custom_target(recursive_include_plan_template
    COMMAND ./recursive_include --roots ${src_dir} ${libs_dir} ${CMAKE_BINARY_DIR}/libs/gflags/include ${GLOG_INCLUDE_DIR} --start ${src_dir}/query/plan_template_cpp --copy ${CMAKE_BINARY_DIR}/include
    DEPENDS ${src_dir}/query/plan_template_cpp
    SOURCES ${src_dir}/query/plan_template_cpp
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/cmake
)
add_dependencies(recursive_include_plan_template fmt gflags glog)
add_dependencies(memgraph_lib recursive_include_plan_template)

# memgraph build name
execute_process(
    OUTPUT_VARIABLE COMMIT_BRANCH
    COMMAND git rev-parse --abbrev-ref HEAD
)
execute_process(
    OUTPUT_VARIABLE COMMIT_HASH
    COMMAND git rev-parse --short HEAD
)
execute_process(
    OUTPUT_VARIABLE COMMIT_NO
    COMMAND git rev-list --count HEAD
)
string(STRIP ${COMMIT_BRANCH} COMMIT_BRANCH)
string(STRIP ${COMMIT_NO} COMMIT_NO)
string(STRIP ${COMMIT_HASH} COMMIT_HASH)
set(MEMGRAPH_BUILD_NAME
    "memgraph_${COMMIT_NO}_${COMMIT_HASH}_${COMMIT_BRANCH}_${CMAKE_BUILD_TYPE}")
add_custom_target(memgraph_link_target ALL
  COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_BINARY_DIR}/${MEMGRAPH_BUILD_NAME} ${CMAKE_BINARY_DIR}/memgraph DEPENDS ${MEMGRAPH_BUILD_NAME})
# -----------------------------------------------------------------------------

# memgraph main executable
if (MEMGRAPH)
    add_executable(${MEMGRAPH_BUILD_NAME} ${src_dir}/memgraph_bolt.cpp)
    set_property(TARGET ${MEMGRAPH_BUILD_NAME}
                 PROPERTY CXX_STANDARD ${cxx_standard})
    target_link_libraries(${MEMGRAPH_BUILD_NAME} memgraph_lib)
endif()

# make CLion aware of all source files so we get refactoring etc
# this target won't be built
file(GLOB_RECURSE __SOURCES ${CMAKE_SOURCE_DIR}/src/*.hpp
                            ${CMAKE_SOURCE_DIR}/src/*.cpp)
add_executable(__refactor_target ${__SOURCES})
set_target_properties(__refactor_target PROPERTIES EXCLUDE_FROM_ALL 1)

get_target_cxx_flags(memgraph_lib compile_flags)
