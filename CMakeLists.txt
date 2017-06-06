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
# -----------------------------------------------------------------------------

# dir variables
set(src_dir ${CMAKE_SOURCE_DIR}/src)
set(libs_dir ${CMAKE_SOURCE_DIR}/libs)
set(tests_dir ${CMAKE_SOURCE_DIR}/tests)
set(include_dir ${CMAKE_SOURCE_DIR}/include)
set(build_include_dir ${CMAKE_BINARY_DIR}/include)
set(test_include_dir ${CMAKE_BINARY_DIR}/tests/include)
set(test_src_dir ${CMAKE_BINARY_DIR}/tests/src)
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

# compiler specific flags
if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    # set(CMAKE_CXX_FLAGS_DEBUG "-Wl,--export-dynamic ${CMAKE_CXX_FLAGS_DEBUG}")
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    # set(CMAKE_CXX_FLAGS_DEBUG "-rdynamic ${CMAKE_CXX_FLAGS_DEBUG}")
endif()

# default build type is debug
if ("${CMAKE_BUILD_TYPE}" STREQUAL "")
    set(CMAKE_BUILD_TYPE "debug")
endif()
message(STATUS "CMake build type: ${CMAKE_BUILD_TYPE}")
# -----------------------------------------------------------------------------

# setup external dependencies -------------------------------------------------
add_subdirectory(libs)
# fmt
set(fmt_source_dir ${libs_dir}/fmt)
set(fmt_static_lib ${fmt_source_dir}/fmt/libfmt.a)
# yaml-cpp
set(yaml_source_dir ${libs_dir}/yaml-cpp)
set(yaml_include_dir ${yaml_source_dir}/include)
set(yaml_static_lib ${yaml_source_dir}/libyaml-cpp.a)

# prepare template and destination folders for query engine (tests)
# and memgraph server binary
# copy query_engine template file
set(query_engine_template_file ${src_dir}/query/plan_template_cpp)
FILE(COPY ${query_engine_template_file}
    DESTINATION ${CMAKE_BINARY_DIR}/tests/template)
FILE(COPY ${query_engine_template_file}
    DESTINATION ${CMAKE_BINARY_DIR}/tests/integration/template)
FILE(COPY ${query_engine_template_file}
    DESTINATION ${CMAKE_BINARY_DIR}/tests/manual/template)
FILE(COPY ${query_engine_template_file}
    DESTINATION ${CMAKE_BINARY_DIR}/template)
# create destination folder for compiled queries
FILE(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/tests/integration/compiled)
FILE(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/tests/manual/compiled)
FILE(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/tests/compiled)
FILE(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/compiled)
# copy hardcoded queries
FILE(COPY ${tests_dir}/integration/hardcoded_query
     DESTINATION ${CMAKE_BINARY_DIR}/tests/integration)
FILE(COPY ${tests_dir}/integration/stream
     DESTINATION ${CMAKE_BINARY_DIR}/tests/integration)
# -----------------------------------------------------------------------------

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
        -I${CMAKE_SOURCE_DIR}/include -I${fmt_source_dir} -I${yaml_include_dir}
    )
endif()
# -----------------------------------------------------------------------------

# logging levels --------------------------------------------------------------
option(LOG_NO_TRACE "Disable trace logging" OFF)
message(STATUS "LOG_NO_TRACE: ${LOG_NO_TRACE}")
if (LOG_NO_TRACE)
    add_definitions(-DLOG_NO_TRACE)
endif()

option(LOG_NO_DEBUG "Disable debug logging" OFF)
message(STATUS "LOG_NO_DEBUG: ${LOG_NO_DEBUG}")
if (LOG_NO_DEBUG)
    add_definitions(-DLOG_NO_DEBUG)
endif()

option(LOG_NO_INFO "Disable info logging" OFF)
message(STATUS "LOG_NO_INFO: ${LOG_NO_INFO}")
if (LOG_NO_INFO)
    add_definitions(-DLOG_NO_INFO)
endif()

option(LOG_NO_WARN "Disable warn logging" OFF)
message(STATUS "LOG_NO_WARN: ${LOG_NO_WARN}")
if (LOG_NO_WARN)
    add_definitions(-DLOG_NO_WARN)
endif()

option(LOG_NO_ERROR "Disable error logging" OFF)
message(STATUS "LOG_NO_ERROR: ${LOG_NO_ERROR}")
if (LOG_NO_ERROR)
    add_definitions(-DLOG_NO_ERROR)
endif()
# -----------------------------------------------------------------------------

# logger type
# the default logger is sync logger
# on: cmake ... -DSYNC_LOGGER=OFF ... async logger is going to be used
option(SYNC_LOGGER "Sync logger" ON)
message(STATUS "SYNC_LOGGER: ${SYNC_LOGGER}")
if (SYNC_LOGGER)
    add_definitions(-DSYNC_LOGGER)
endif()
# -----------------------------------------------------------------------------

# custom assert control parameters

# Debug assert, if value is OFF debug asserts will be inactive.
# Default value is ON.
option(DEBUG_ASSERT "Enable debug assertions" ON)
message(STATUS "DEBUG_ASSERT: ${DEBUG_ASSERT}")
if(DEBUG_ASSERT)
    add_definitions(-DDEBUG_ASSERT_ON)
endif()

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
# tests
option(ALL_TESTS "Add all test binaries" ON)
message(STATUS "Add all test binaries: ${ALL_TESTS}")
option(BENCHMARK_TESTS "Add benchmark test binaries" OFF)
message(STATUS "Add benchmark test binaries: ${BENCHMARK_TESTS}")
option(CONCURRENT_TESTS "Add concurrent test binaries" OFF)
message(STATUS "Add concurrent test binaries: ${CONCURRENT_TESTS}")
option(INTEGRATION_TESTS "Add integration test binaries" OFF)
message(STATUS "Add integration test binaries: ${INTEGRATION_TESTS}")
option(MANUAL_TESTS "Add manual test binaries" OFF)
message(STATUS "Add manual test binaries: ${MANUAL_TESTS}")
option(UNIT_TESTS "Add unit test binaries" OFF)
message(STATUS "Add unit test binaries: ${UNIT_TESTS}")
option(HARDCODED_TARGETS "Make hardcoded query targets" ON)
message(STATUS "Make hardcoded query targets: ${HARDCODED_TARGETS}")
option(TEST_COVERAGE "Generate coverage reports from unit tests" OFF)
message(STATUS "Generate coverage from unit tests: ${TEST_COVERAGE}")
# -----------------------------------------------------------------------------

# includes
include_directories(${src_dir})
include_directories(${build_include_dir})
include_directories(SYSTEM ${fmt_source_dir})
include_directories(SYSTEM ${yaml_include_dir})
include_directories(SYSTEM ${GTEST_INCLUDE_DIRS} ${GMOCK_INCLUDE_DIRS})
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/libs)
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
    ${src_dir}/config/config.cpp
    ${src_dir}/dbms/dbms.cpp
#    ${src_dir}/dbms/cleaner.cpp
    ${src_dir}/utils/numerics/saturate.cpp
    ${src_dir}/io/network/addrinfo.cpp
    ${src_dir}/io/network/network_endpoint.cpp
    ${src_dir}/io/network/socket.cpp
    ${src_dir}/threading/thread.cpp
    ${src_dir}/mvcc/id.cpp
    ${src_dir}/durability/snapshooter.cpp
    ${src_dir}/durability/recovery.cpp
#    ${src_dir}/snapshot/snapshot_engine.cpp
#    ${src_dir}/snapshot/snapshoter.cpp
#    ${src_dir}/snapshot/snapshot_encoder.cpp
#    ${src_dir}/snapshot/snapshot_decoder.cpp
    ${src_dir}/storage/property_value.cpp
    ${src_dir}/storage/locking/record_lock.cpp
#    ${src_dir}/storage/garbage/garbage.cpp
    ${src_dir}/storage/record_accessor.cpp
    ${src_dir}/storage/vertex_accessor.cpp
    ${src_dir}/storage/edge_accessor.cpp
#    ${src_dir}/storage/record_accessor.cpp
    ${src_dir}/transactions/snapshot.cpp
    ${src_dir}/transactions/transaction.cpp
    ${src_dir}/template_engine/engine.cpp
    ${src_dir}/logging/streams/stdout.cpp
    ${src_dir}/logging/streams/stderr.cpp
    ${src_dir}/logging/levels.cpp
    ${src_dir}/logging/logs/sync_log.cpp
    ${src_dir}/logging/logs/async_log.cpp
    ${src_dir}/logging/default.cpp
    ${src_dir}/logging/log.cpp
    ${src_dir}/database/graph_db.cpp
    ${src_dir}/database/graph_db_accessor.cpp
    ${src_dir}/query/stripper.cpp
    ${src_dir}/query/console.cpp
    ${src_dir}/query/frontend/ast/cypher_main_visitor.cpp
    ${src_dir}/query/typed_value.cpp
    ${src_dir}/query/interpret/awesome_memgraph_functions.cpp
    ${src_dir}/query/plan/operator.cpp
    ${src_dir}/query/plan/rule_based_planner.cpp
    ${src_dir}/query/plan/variable_start_planner.cpp
    ${src_dir}/query/plan/cost_estimator.cpp
    ${src_dir}/query/frontend/semantic/symbol_generator.cpp
)
# -----------------------------------------------------------------------------

# STATIC library used by memgraph executables
add_library(memgraph_lib STATIC ${memgraph_src_files})
target_link_libraries(memgraph_lib stdc++fs)
add_dependencies(memgraph_lib generate_opencypher_parser
                              generate_plan_compiler_flags)
# executables that require memgraph_lib should link MEMGRAPH_ALL_LIBS to link all dependant libraries
set(MEMGRAPH_ALL_LIBS gflags memgraph_lib stdc++fs Threads::Threads fmt yaml-cpp antlr_opencypher_parser_lib dl)
if (READLINE_FOUND)
    list(APPEND MEMGRAPH_ALL_LIBS ${READLINE_LIBRARY})
endif()
# -----------------------------------------------------------------------------

# STATIC PIC library used by query engine
add_library(memgraph_pic STATIC ${memgraph_src_files})
target_link_libraries(memgraph_pic stdc++fs)
add_dependencies(memgraph_pic generate_opencypher_parser
                              generate_plan_compiler_flags)
set_property(TARGET memgraph_pic PROPERTY POSITION_INDEPENDENT_CODE TRUE)
# -----------------------------------------------------------------------------

# proof of concepts
if (POC)
    add_subdirectory(poc)
endif()
# -----------------------------------------------------------------------------

# tests
if (ALL_TESTS OR BENCHMARK_TESTS OR CONCURRENT_TEST OR INTEGRATION_TEST
        OR MANUAL_TESTS OR UNIT_TESTS)
    add_subdirectory(tests)
endif()
# -----------------------------------------------------------------------------

execute_process(
    COMMAND ./recursive_include --roots ${src_dir} ${libs_dir} --start ${src_dir}/query/plan_template_cpp --copy ${CMAKE_BINARY_DIR}/include
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/cmake
)

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
# -----------------------------------------------------------------------------

# memgraph main executable
if (MEMGRAPH)
    add_executable(${MEMGRAPH_BUILD_NAME} ${src_dir}/memgraph_bolt.cpp)
    set_property(TARGET ${MEMGRAPH_BUILD_NAME}
                 PROPERTY CXX_STANDARD ${cxx_standard})
    target_link_libraries(${MEMGRAPH_BUILD_NAME} ${MEMGRAPH_ALL_LIBS})
endif()

# utility target to copy hardcoded queries
# FROM: tests/integration/hardcoded_query TO: build/compiled/hardcode
add_executable(__copy_hardcoded_queries ${src_dir}/copy_hardcoded_queries.cpp)
set_property(TARGET __copy_hardcoded_queries PROPERTY CXX_STANDARD
        ${cxx_standard})
target_link_libraries(__copy_hardcoded_queries ${MEMGRAPH_ALL_LIBS})
add_custom_target(copy_hardcoded_queries ./__copy_hardcoded_queries --src
        ${CMAKE_SOURCE_DIR}/tests/integration/hardcoded_query --dst
        ${CMAKE_BINARY_DIR}/compiled/hardcode
        WORKING_DIR ${CMAKE_BINARY_DIR})

# make CLion aware of all source files so we get refactoring etc
# this target won't be built
file(GLOB_RECURSE __SOURCES ${CMAKE_SOURCE_DIR}/src/*.hpp
                            ${CMAKE_SOURCE_DIR}/src/*.cpp)
add_executable(__refactor_target ${__SOURCES})
set_target_properties(__refactor_target PROPERTIES EXCLUDE_FROM_ALL 1)

# targets to check compilability of all hardcoded query plans
# that is a first step in integration testing
# integration testing phases should be
#   1. compilation of all hardcoded query plans
#   2. query plan execution agains empty database and injected OutputStream
#   3. integration tests for all pilot/clients written in cucumber
# the following targets address only the first phase
if(HARDCODED_TARGETS)
  file(GLOB __HARDCODED_SOURCES
       ${CMAKE_SOURCE_DIR}/tests/integration/hardcoded_query/*.cpp)
  foreach(file_path ${__HARDCODED_SOURCES})
      get_filename_component(file_name ${file_path} NAME_WE)
      set(target_name __${file_name}_hardcoded_target)
      add_executable(${target_name} ${CMAKE_SOURCE_DIR}/libs/__main.cpp
                                    ${file_path})
      target_link_libraries(${target_name} ${MEMGRAPH_ALL_LIBS})
      set_property(TARGET ${target_name} PROPERTY CXX_STANDARD ${cxx_standard})
      set_target_properties(${target_name}
                            PROPERTIES RUNTIME_OUTPUT_DIRECTORY
                            "${CMAKE_BINARY_DIR}/__hardcoded_targets")
  endforeach()
endif()

get_target_cxx_flags(memgraph_lib compile_flags)
set(plan_compiler_flags_file ${build_include_dir}/query/plan_compiler_flags.hpp)
add_custom_target(generate_plan_compiler_flags
  COMMENT
  "Generating ${plan_compiler_flags_file} with compile flags: ${compile_flags}"
  VERBATIM
  COMMAND
  /bin/echo -e
  "#pragma once"
  "\\n// Generated from cmake. Flags are taken from current build configuration."
  "\\n// Do not edit this manually!"
  "\\nstatic constexpr const char compile_flags[] = \"${compile_flags}\";"
  "\\nstatic constexpr const char include_dirs[] ="
  "\"-I${CMAKE_BINARY_DIR}/include -I${CMAKE_SOURCE_DIR}/libs/fmt\";"
  "\\nstatic constexpr const char link_dirs[] = \"-L${CMAKE_BINARY_DIR}\";"
  > ${plan_compiler_flags_file}
  BYPRODUCTS ${plan_compiler_flags_file}
)
set_directory_properties(PROPERTIES ADDITIONAL_MAKE_CLEAN_FILES ${plan_compiler_flags_file})
