# MemGraph CMake configuration

cmake_minimum_required(VERSION 3.1)

# !! IMPORTANT !! run ./project_root/init.sh before cmake command
# to download dependencies

# choose a compiler 
# NOTE: must be choosen before use of project() or enable_language()
if (UNIX)
set(CMAKE_C_COMPILER "clang")
set(CMAKE_CXX_COMPILER "clang++")
endif (UNIX)
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

# threading
find_package(Threads REQUIRED)
# -----------------------------------------------------------------------------

# c++14
set(cxx_standard 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y")
# -----------------------------------------------------------------------------

# dir variables
set(src_dir ${CMAKE_SOURCE_DIR}/src)
set(libs_dir ${CMAKE_SOURCE_DIR}/libs)
set(include_dir ${CMAKE_SOURCE_DIR}/include)
set(build_include_dir ${CMAKE_BINARY_DIR}/include)
set(test_include_dir ${CMAKE_BINARY_DIR}/tests/include)
set(test_src_dir ${CMAKE_BINARY_DIR}/tests/src)
# -----------------------------------------------------------------------------

# setup external dependencies
# lemon & lempar
set(lemon_dir ${libs_dir}/lemon)
# lexertl
set(lexertl_dir ${libs_dir}/lexertl)
# fmt
set(fmt_source_dir ${libs_dir}/fmt)
set(fmt_static_lib ${fmt_source_dir}/fmt/libfmt.a)
# yaml-cpp
set(yaml_source_dir ${libs_dir}/yaml-cpp)
set(yaml_include_dir ${yaml_source_dir}/include)
set(yaml_static_lib ${yaml_source_dir}/libyaml-cpp.a)
# Catch (C++ Automated Test Cases in Headers)
set(catch_source_dir "${libs_dir}/Catch")
# -----------------------------------------------------------------------------

# load cmake modules: cmake/*.cmake
include(gtest)
include(gbenchmark)
# -----------------------------------------------------------------------------

# build memgraph's cypher grammar
# copy grammar file to the build directory
FILE(COPY ${include_dir}/query/language/cypher/cypher.y
     DESTINATION ${CMAKE_BINARY_DIR})
# build cypher parser (only c file - cypher.c)
EXECUTE_PROCESS(
    COMMAND ${lemon_dir}/lemon ${CMAKE_BINARY_DIR}/cypher.y -s
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)
# change cypher parser c extension to cpp (cypher.c -> cypher.cpp)
FILE(RENAME ${CMAKE_BINARY_DIR}/cypher.c ${CMAKE_BINARY_DIR}/cypher.cpp)
# add include file (cypher.h) to build include dir
SET(cypher_build_include_dir ${build_include_dir}/cypher)
FILE(MAKE_DIRECTORY ${cypher_build_include_dir})
FILE(RENAME ${CMAKE_BINARY_DIR}/cypher.h ${cypher_build_include_dir}/cypher.h)
# -----------------------------------------------------------------------------

# prepare template and destination folders for query engine (tests)
# and memgraph server binary
# copy query_engine's templates file
FILE(COPY ${src_dir}/query_engine/template
     DESTINATION ${CMAKE_BINARY_DIR}/tests)
FILE(COPY ${src_dir}/query_engine/template DESTINATION ${CMAKE_BINARY_DIR})
# create destination folder for compiled queries
FILE(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/tests/compiled/cpu)
FILE(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/compiled/cpu)
# -----------------------------------------------------------------------------

# copy files needed for query engine (headers)
include(copy_includes)
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

# TODO: add specific flags
# release flags
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")
#debug flags
set(CMAKE_CXX_FLAGS_DEBUG "-g")

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

# logging levels
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
# TODO: find a way how to applay those defines at the query compile time
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

# Runtime assert, if value is OFF runtime asserts will be inactive during
# runtime. Default value is ON.
option(RUNTIME_ASSERT "Enable runtime assertions" ON)
message(STATUS "RUNTIME_ASSERT: ${RUNTIME_ASSERT}")
if(RUNTIME_ASSERT)
    add_definitions(-DRUNTIME_ASSERT_ON)
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
# -----------------------------------------------------------------------------

# includes
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${src_dir})
include_directories(${build_include_dir})
include_directories(${fmt_source_dir})
include_directories(${yaml_include_dir})
include_directories(${http_parser_source_dir})
include_directories(${lexertl_dir})
include_directories(${libuv_source_dir}/include)
include_directories(${rapidjson_source_dir}/include)
include_directories(${r3_source_dir}/include)
# -----------------------------------------------------------------------------

# creates build/libcypher_lib.a
add_library(cypher_lib STATIC ${CMAKE_BINARY_DIR}/cypher.cpp)
# -----------------------------------------------------------------------------

# TODO: remove from here (isolate HTTP server)
# # REST API preprocessor
# EXECUTE_PROCESS(
#     COMMAND python link_resources.py
#     WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/api
# )
# # ---------------------------------------------------------------------------

# all memgraph src files
set(memgraph_src_files
    ${src_dir}/config/config.cpp
    ${src_dir}/dbms/dbms.cpp
#    ${src_dir}/dbms/cleaner.cpp
    ${src_dir}/utils/string/transform.cpp
    ${src_dir}/utils/string/join.cpp
    ${src_dir}/utils/string/file.cpp
    ${src_dir}/utils/numerics/saturate.cpp
    ${src_dir}/communication/bolt/v1/bolt.cpp
    ${src_dir}/communication/bolt/v1/states.cpp
    ${src_dir}/communication/bolt/v1/session.cpp
    ${src_dir}/communication/bolt/v1/states/error.cpp
    ${src_dir}/communication/bolt/v1/states/executor.cpp
    ${src_dir}/communication/bolt/v1/states/init.cpp
    ${src_dir}/communication/bolt/v1/states/handshake.cpp
    ${src_dir}/communication/bolt/v1/transport/bolt_decoder.cpp
    ${src_dir}/communication/bolt/v1/transport/buffer.cpp
    ${src_dir}/communication/bolt/v1/serialization/bolt_serializer.cpp
    ${src_dir}/threading/thread.cpp
    ${src_dir}/mvcc/id.cpp
#    ${src_dir}/snapshot/snapshot_engine.cpp
#    ${src_dir}/snapshot/snapshoter.cpp
#    ${src_dir}/snapshot/snapshot_encoder.cpp
#    ${src_dir}/snapshot/snapshot_decoder.cpp
    ${src_dir}/storage/typed_value.cpp
    ${src_dir}/storage/typed_value_store.cpp
    ${src_dir}/storage/locking/record_lock.cpp
#    ${src_dir}/storage/garbage/garbage.cpp
    ${src_dir}/storage/vertex_accessor.cpp
        ${src_dir}/storage/edge_accessor.cpp
#        ${src_dir}/storage/record_accessor.cpp
    ${src_dir}/transactions/snapshot.cpp
    ${src_dir}/transactions/transaction.cpp
    ${src_dir}/transactions/transaction_read.cpp
    ${src_dir}/template_engine/engine.cpp
    ${src_dir}/logging/streams/stdout.cpp
    ${src_dir}/logging/levels.cpp
    ${src_dir}/logging/logs/sync_log.cpp
    ${src_dir}/logging/logs/async_log.cpp
    ${src_dir}/logging/default.cpp
    ${src_dir}/logging/log.cpp
    ${src_dir}/io/network/tls.cpp
    ${src_dir}/database/graph_db.cpp
    ${src_dir}/database/graph_db_accessor.cpp
)
# -----------------------------------------------------------------------------

# STATIC library used by memgraph executables
add_library(memgraph_lib STATIC ${memgraph_src_files})
# -----------------------------------------------------------------------------

# STATIC PIC library used by query engine
add_library(memgraph_pic STATIC ${memgraph_src_files})
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

    target_link_libraries(${MEMGRAPH_BUILD_NAME} memgraph_lib)
    target_link_libraries(${MEMGRAPH_BUILD_NAME} stdc++fs)
    target_link_libraries(${MEMGRAPH_BUILD_NAME} Threads::Threads)
    target_link_libraries(${MEMGRAPH_BUILD_NAME} cypher_lib)

    if (UNIX)
        target_link_libraries(${MEMGRAPH_BUILD_NAME} crypto)
        # target_link_libraries(${MEMGRAPH_BUILD_NAME} ssl)
        target_link_libraries(${MEMGRAPH_BUILD_NAME} ${fmt_static_lib})
        target_link_libraries(${MEMGRAPH_BUILD_NAME} ${yaml_static_lib})
        target_link_libraries(${MEMGRAPH_BUILD_NAME} dl)
    endif (UNIX)
endif()
# -----------------------------------------------------------------------------
