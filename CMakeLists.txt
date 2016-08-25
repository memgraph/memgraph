cmake_minimum_required(VERSION 3.1)

# get directory name
get_filename_component(ProjectId ${CMAKE_SOURCE_DIR} NAME)
# replace whitespaces with underscores
string(REPLACE " " "_" ProjectId ${ProjectId})
# set project name
project(${ProjectId})

find_package(Threads REQUIRED)

# flags

# c++14
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y")

# glibcxx debug (useful for gdb)
# the problem is that the query engine doesn't work as it should work if
# this flag is present
# TODO: find more appropriate way to use this flag only when it is needed
# set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_GLIBCXX_DEBUG")

# functions

# prints all included directories
function(list_includes)
    get_property(dirs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                      PROPERTY INCLUDE_DIRECTORIES)
    foreach(dir ${dirs})
          message(STATUS "dir='${dir}'")
    endforeach()
endfunction(list_includes)

# get file names from list of file paths
function(get_file_names file_paths file_names)
    set(file_names "")
    foreach(file_path ${file_paths})
        get_filename_component (file_name ${file_path} NAME_WE)
        list(APPEND file_names ${file_name})
    endforeach()
    set(file_names "${file_names}" PARENT_SCOPE)
endfunction()

MACRO(SUBDIRLIST result curdir)
    FILE(GLOB children RELATIVE ${curdir} ${curdir}/*)
    SET(dirlist "")
    FOREACH(child ${children})
        IF(IS_DIRECTORY ${curdir}/${child})
            LIST(APPEND dirlist ${child})
        ENDIF()
    ENDFOREACH()
    SET(${result} ${dirlist})
ENDMACRO()

# custom targets

# move test data data to the build directory
if (UNIX)
    set(test_data "tests/data")
    set(test_data_src "${CMAKE_SOURCE_DIR}/${test_data}")
	set(test_data_dst "${CMAKE_BINARY_DIR}/${test_data}")
	add_custom_target (test_data
		COMMAND rm -rf ${test_data_dst}
        COMMAND cp -r ${test_data_src} ${test_data_dst}
	)
endif (UNIX)

# external dependencies

set(src_dir ${CMAKE_SOURCE_DIR}/src)
set(libs_dir ${CMAKE_SOURCE_DIR}/libs)
set(include_dir ${CMAKE_SOURCE_DIR}/include)
set(build_include_dir ${CMAKE_BINARY_DIR}/include)
set(test_include_dir ${CMAKE_BINARY_DIR}/tests/include)
set(test_src_dir ${CMAKE_BINARY_DIR}/tests/src)

# setup external dependencies

# !! IMPORTANT !! run ./libs/setup.sh before cmake command
# TODO: run from execute_process

# lemon & lempar
set(lemon_dir ${libs_dir}/lemon)
# lexertl
set(lexertl_dir ${libs_dir}/lexertl)
# fmt
set(fmt_source_dir ${libs_dir}/fmt)
set(fmt_static_lib ${fmt_source_dir}/fmt/libfmt.a)
# r3
set(r3_source_dir ${libs_dir}/r3)
set(r3_static_lib ${r3_source_dir}/.libs/libr3.a)
# http-parser
set(http_parser_source_dir "${libs_dir}/http-parser")
set(http_parser_static_lib ${http_parser_source_dir}/libhttp_parser.a)
# libuv
set(libuv_source_dir ${libs_dir}/libuv)
set(libuv_static_lib ${libuv_source_dir}/.libs/libuv.a)
# rapidjson (C++ JSON encoder/decoder)
set(rapidjson_source_dir "${libs_dir}/rapidjson")
# Catch (C++ Automated Test Cases in Headers)
set(catch_source_dir "${libs_dir}/Catch")

# build memgraph's cypher grammar
# copy grammar file to the build directory
FILE(COPY ${src_dir}/cypher/cypher.y DESTINATION ${CMAKE_BINARY_DIR})
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

# prepare template and destination folders for query engine (tests)
# and memgraph server binary
# copy query_engine's templates file
FILE(COPY ${src_dir}/query_engine/template DESTINATION ${CMAKE_BINARY_DIR}/tests)
FILE(COPY ${src_dir}/query_engine/template DESTINATION ${CMAKE_BINARY_DIR})
# create destination folder for compiled queries
FILE(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/tests/compiled/cpu)
FILE(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/compiled/cpu)

# TODO: filter header files, all files don't need to be copied
# they are all copied because query engine needs header files during
# query compilation
# TODO: make a function (REMOVE copy pasted part)
# SUBDIRLIST(source_folders ${src_dir})
# foreach(source_folder ${source_folders})
#     file(COPY ${src_dir}/${source_folder} DESTINATION ${build_include_dir})
# endforeach()
SUBDIRLIST(source_folders ${src_dir})
foreach(source_folder ${source_folders})
    file(COPY ${src_dir}/${source_folder} DESTINATION ${test_src_dir})
endforeach()
SUBDIRLIST(source_folders ${include_dir})
foreach(source_foler ${source_folders})
    file(COPY ${include_dir}/${source_folder} DESTINATION ${test_include_dir})
endforeach()

# -----------------------------------------------------------------------------
# COPY header files required by query engine (query compiler)
# TODO: somehow automate (in destination dir should be only required include files)
FILE(COPY ${include_dir}/database/db.hpp DESTINATION ${build_include_dir}/database)
FILE(COPY ${include_dir}/database/db_transaction.hpp DESTINATION ${build_include_dir}/database)
FILE(COPY ${include_dir}/database/db_accessor.hpp DESTINATION ${build_include_dir}/database)

FILE(COPY ${include_dir}/storage/common.hpp DESTINATION ${build_include_dir}/storage)
FILE(COPY ${include_dir}/storage/graph.hpp DESTINATION ${build_include_dir}/storage)
FILE(COPY ${include_dir}/storage/edge.hpp DESTINATION ${build_include_dir}/storage)
FILE(COPY ${include_dir}/storage/edge_record.hpp DESTINATION ${build_include_dir}/storage)
FILE(COPY ${include_dir}/storage/edge_accessor.hpp DESTINATION ${build_include_dir}/storage)
FILE(COPY ${include_dir}/storage/edges.hpp DESTINATION ${build_include_dir}/storage)
FILE(COPY ${include_dir}/storage/vertices.hpp DESTINATION ${build_include_dir}/storage)
FILE(COPY ${include_dir}/storage/vertex.hpp DESTINATION ${build_include_dir}/storage)
FILE(COPY ${include_dir}/storage/vertex_accessor.hpp DESTINATION ${build_include_dir}/storage)
FILE(COPY ${include_dir}/storage/record_accessor.hpp DESTINATION ${build_include_dir}/storage)
FILE(COPY ${include_dir}/storage/locking/record_lock.hpp DESTINATION ${build_include_dir}/storage/locking)
FILE(COPY ${include_dir}/storage/locking/lock_status.hpp DESTINATION ${build_include_dir}/storage/locking)
FILE(COPY ${include_dir}/storage/edge_type/edge_type_store.hpp DESTINATION ${build_include_dir}/storage/edge_type)
FILE(COPY ${include_dir}/storage/edge_type/edge_type.hpp DESTINATION ${build_include_dir}/storage/edge_type)
FILE(COPY ${include_dir}/storage/label/label_store.hpp DESTINATION ${build_include_dir}/storage/label)
FILE(COPY ${include_dir}/storage/model/edge_map.hpp DESTINATION ${build_include_dir}/storage/model)
FILE(COPY ${include_dir}/storage/model/properties/flags.hpp DESTINATION ${build_include_dir}/storage/model/properties)

FILE(COPY ${include_dir}/query_engine/util.hpp DESTINATION ${build_include_dir}/query_engine)
FILE(COPY ${include_dir}/query_engine/i_code_cpu.hpp DESTINATION ${build_include_dir}/query_engine)
FILE(COPY ${include_dir}/query_engine/query_result.hpp DESTINATION ${build_include_dir}/query_engine)
FILE(COPY ${include_dir}/query_engine/query_stripped.hpp DESTINATION ${build_include_dir}/query_engine)

FILE(COPY ${include_dir}/data_structures/concurrent/concurrent_map.hpp DESTINATION ${build_include_dir}/data_structures/concurrent)
FILE(COPY ${include_dir}/data_structures/concurrent/concurrent_set.hpp DESTINATION ${build_include_dir}/data_structures/concurrent)
FILE(COPY ${include_dir}/data_structures/concurrent/concurrent_list.hpp DESTINATION ${build_include_dir}/data_structures/concurrent)
FILE(COPY ${include_dir}/data_structures/concurrent/common.hpp DESTINATION ${build_include_dir}/data_structures/concurrent)
FILE(COPY ${include_dir}/data_structures/concurrent/skiplist.hpp DESTINATION ${build_include_dir}/data_structures/concurrent)
FILE(COPY ${include_dir}/data_structures/concurrent/skiplist_gc.hpp DESTINATION ${build_include_dir}/data_structures/concurrent)
FILE(COPY ${include_dir}/data_structures/map/rh_hashmultimap.hpp DESTINATION ${build_include_dir}/data_structures/map)
FILE(COPY ${include_dir}/data_structures/map/rh_common.hpp DESTINATION ${build_include_dir}/data_structures/map)

FILE(COPY ${include_dir}/data_structures/bitset/dynamic_bitset.hpp DESTINATION ${build_include_dir}/data_structures/bitset)

FILE(COPY ${include_dir}/threading/sync/lockable.hpp DESTINATION ${build_include_dir}/threading/sync)
FILE(COPY ${include_dir}/threading/sync/spinlock.hpp DESTINATION ${build_include_dir}/threading/sync)
FILE(COPY ${include_dir}/threading/sync/futex.hpp DESTINATION ${build_include_dir}/threading/sync)
FILE(COPY ${include_dir}/threading/sync/lock_timeout_error.hpp DESTINATION ${build_include_dir}/threading/sync)

FILE(COPY ${include_dir}/memory/freelist.hpp DESTINATION ${build_include_dir}/memory)
FILE(COPY ${include_dir}/memory/lazy_gc.hpp DESTINATION ${build_include_dir}/memory)

FILE(COPY ${include_dir}/mvcc/cre_exp.hpp DESTINATION ${build_include_dir}/mvcc)
FILE(COPY ${include_dir}/mvcc/hints.hpp DESTINATION ${build_include_dir}/mvcc)
FILE(COPY ${include_dir}/mvcc/id.hpp DESTINATION ${build_include_dir}/mvcc)
FILE(COPY ${include_dir}/mvcc/mvcc_error.hpp DESTINATION ${build_include_dir}/mvcc)
FILE(COPY ${include_dir}/mvcc/record.hpp DESTINATION ${build_include_dir}/mvcc)
FILE(COPY ${include_dir}/mvcc/serialization_error.hpp DESTINATION ${build_include_dir}/mvcc)
FILE(COPY ${include_dir}/mvcc/version.hpp DESTINATION ${build_include_dir}/mvcc)
FILE(COPY ${include_dir}/mvcc/version_list.hpp DESTINATION ${build_include_dir}/mvcc)

FILE(COPY ${include_dir}/transactions/transaction.hpp DESTINATION ${build_include_dir}/transactions)
FILE(COPY ${include_dir}/transactions/lock_store.hpp DESTINATION ${build_include_dir}/transactions)
FILE(COPY ${include_dir}/transactions/snapshot.hpp DESTINATION ${build_include_dir}/transactions)
FILE(COPY ${include_dir}/transactions/commit_log.hpp DESTINATION ${build_include_dir}/transactions)
FILE(COPY ${include_dir}/transactions/engine.hpp DESTINATION ${build_include_dir}/transactions)
FILE(COPY ${include_dir}/transactions/transaction_store.hpp DESTINATION ${build_include_dir}/transactions)

FILE(COPY ${include_dir}/storage/model/properties/properties.hpp DESTINATION ${build_include_dir}/storage/model/properties)
FILE(COPY ${include_dir}/storage/model/properties/property.hpp DESTINATION ${build_include_dir}/storage/model/properties)
FILE(COPY ${include_dir}/storage/model/properties/traversers/consolewriter.hpp DESTINATION ${build_include_dir}/storage/model/properties/traversers)
FILE(COPY ${include_dir}/storage/model/properties/traversers/jsonwriter.hpp DESTINATION ${build_include_dir}/storage/model/properties/traversers)
FILE(COPY ${include_dir}/storage/model/properties/handler.hpp DESTINATION ${build_include_dir}/storage/model/properties)
FILE(COPY ${include_dir}/storage/model/properties/all.hpp DESTINATION ${build_include_dir}/storage/model/properties)
FILE(COPY ${include_dir}/storage/model/properties/bool.hpp DESTINATION ${build_include_dir}/storage/model/properties)
FILE(COPY ${include_dir}/storage/model/properties/float.hpp DESTINATION ${build_include_dir}/storage/model/properties)
FILE(COPY ${include_dir}/storage/model/properties/double.hpp DESTINATION ${build_include_dir}/storage/model/properties)
FILE(COPY ${include_dir}/storage/model/properties/int32.hpp DESTINATION ${build_include_dir}/storage/model/properties)
FILE(COPY ${include_dir}/storage/model/properties/int64.hpp DESTINATION ${build_include_dir}/storage/model/properties)
FILE(COPY ${include_dir}/storage/model/properties/string.hpp DESTINATION ${build_include_dir}/storage/model/properties)
FILE(COPY ${include_dir}/storage/model/properties/array.hpp DESTINATION ${build_include_dir}/storage/model/properties)
FILE(COPY ${include_dir}/storage/model/properties/floating.hpp DESTINATION ${build_include_dir}/storage/model/properties)
FILE(COPY ${include_dir}/storage/model/properties/number.hpp DESTINATION ${build_include_dir}/storage/model/properties)
FILE(COPY ${include_dir}/storage/model/properties/integral.hpp DESTINATION ${build_include_dir}/storage/model/properties)
FILE(COPY ${include_dir}/storage/model/properties/property_family.hpp DESTINATION ${build_include_dir}/storage/model/properties)
FILE(COPY ${include_dir}/storage/model/properties/utils/math_operations.hpp DESTINATION ${build_include_dir}/storage/model/properties/utils)
FILE(COPY ${include_dir}/storage/model/properties/utils/unary_negation.hpp DESTINATION ${build_include_dir}/storage/model/properties/utils)
FILE(COPY ${include_dir}/storage/model/properties/utils/modulo.hpp DESTINATION ${build_include_dir}/storage/model/properties/utils)


FILE(COPY ${include_dir}/storage/model/edge_model.hpp DESTINATION ${build_include_dir}/storage/model)
FILE(COPY ${include_dir}/storage/model/property_model.hpp DESTINATION ${build_include_dir}/storage/model)
FILE(COPY ${include_dir}/storage/model/vertex_model.hpp DESTINATION ${build_include_dir}/storage/model)
FILE(COPY ${include_dir}/storage/model/edge_list.hpp DESTINATION ${build_include_dir}/storage/model)

FILE(COPY ${include_dir}/storage/label/label.hpp DESTINATION ${build_include_dir}/storage/label)
FILE(COPY ${include_dir}/storage/label/label_collection.hpp DESTINATION ${build_include_dir}/storage/label)
FILE(COPY ${include_dir}/storage/label/label_store.hpp DESTINATION ${build_include_dir}/storage/label)

FILE(COPY ${include_dir}/storage/indexes/index.hpp DESTINATION ${build_include_dir}/storage/indexes)
FILE(COPY ${include_dir}/storage/indexes/index_record.hpp DESTINATION ${build_include_dir}/storage/indexes)
FILE(COPY ${include_dir}/storage/indexes/index_record_collection.hpp DESTINATION ${build_include_dir}/storage/indexes)
FILE(COPY ${include_dir}/storage/indexes/index_base.hpp DESTINATION ${build_include_dir}/storage/indexes)
FILE(COPY ${include_dir}/storage/indexes/impl/nonunique_unordered_index.hpp DESTINATION ${build_include_dir}/storage/indexes/impl)

FILE(COPY ${include_dir}/utils/sys.hpp DESTINATION ${build_include_dir}/utils)
FILE(COPY ${include_dir}/utils/bswap.hpp DESTINATION ${build_include_dir}/utils)
FILE(COPY ${include_dir}/utils/stacktrace.hpp DESTINATION ${build_include_dir}/utils)
FILE(COPY ${include_dir}/utils/auto_scope.hpp DESTINATION ${build_include_dir}/utils)
FILE(COPY ${include_dir}/utils/assert.hpp DESTINATION ${build_include_dir}/utils)
FILE(COPY ${include_dir}/utils/reference_wrapper.hpp DESTINATION ${build_include_dir}/utils)
FILE(COPY ${include_dir}/utils/underlying_cast.hpp DESTINATION ${build_include_dir}/utils)
FILE(COPY ${include_dir}/utils/total_ordering.hpp DESTINATION ${build_include_dir}/utils)
FILE(COPY ${include_dir}/utils/crtp.hpp DESTINATION ${build_include_dir}/utils)
FILE(COPY ${include_dir}/utils/placeholder.hpp DESTINATION ${build_include_dir}/utils)
FILE(COPY ${include_dir}/utils/likely.hpp DESTINATION ${build_include_dir}/utils)
FILE(COPY ${include_dir}/utils/cpu_relax.hpp DESTINATION ${build_include_dir}/utils)
FILE(COPY ${include_dir}/utils/counters/atomic_counter.hpp DESTINATION ${build_include_dir}/utils/counters)
FILE(COPY ${include_dir}/utils/counters/simple_counter.hpp DESTINATION ${build_include_dir}/utils/counters)
FILE(COPY ${include_dir}/utils/random/fast_binomial.hpp DESTINATION ${build_include_dir}/utils/random)
FILE(COPY ${include_dir}/utils/random/xorshift128plus.hpp DESTINATION ${build_include_dir}/utils/random)
FILE(COPY ${include_dir}/utils/exceptions/basic_exception.hpp DESTINATION ${build_include_dir}/utils/exceptions)
FILE(COPY ${include_dir}/utils/datetime/timestamp.hpp DESTINATION ${build_include_dir}/utils/datetime)
FILE(COPY ${include_dir}/utils/datetime/datetime_error.hpp DESTINATION ${build_include_dir}/utils/datetime)
FILE(COPY ${include_dir}/utils/types/byte.hpp DESTINATION ${build_include_dir}/utils/types)
FILE(COPY ${include_dir}/utils/iterator/iterator_base.hpp DESTINATION ${build_include_dir}/utils/iterator)
FILE(COPY ${include_dir}/utils/option_ptr.hpp DESTINATION ${build_include_dir}/utils)
FILE(COPY ${include_dir}/utils/option.hpp DESTINATION ${build_include_dir}/utils)
FILE(COPY ${include_dir}/utils/border.hpp DESTINATION ${build_include_dir}/utils)

FILE(COPY ${include_dir}/communication/communication.hpp DESTINATION ${build_include_dir}/communication)
FILE(COPY ${include_dir}/communication/bolt/v1/config.hpp DESTINATION ${build_include_dir}/communication/bolt/v1)
FILE(COPY ${include_dir}/communication/bolt/v1/serialization/record_stream.hpp DESTINATION ${build_include_dir}/communication/bolt/v1/serialization)
FILE(COPY ${include_dir}/communication/bolt/v1/serialization/bolt_serializer.hpp DESTINATION ${build_include_dir}/communication/bolt/v1/serialization)
FILE(COPY ${include_dir}/communication/bolt/v1/transport/bolt_encoder.hpp DESTINATION ${build_include_dir}/communication/bolt/v1/transport)
FILE(COPY ${include_dir}/communication/bolt/v1/transport/chunked_buffer.hpp DESTINATION ${build_include_dir}/communication/bolt/v1/transport)
FILE(COPY ${include_dir}/communication/bolt/v1/transport/chunked_encoder.hpp DESTINATION ${build_include_dir}/communication/bolt/v1/transport)
FILE(COPY ${include_dir}/communication/bolt/v1/transport/socket_stream.hpp DESTINATION ${build_include_dir}/communication/bolt/v1/transport)
FILE(COPY ${include_dir}/communication/bolt/v1/transport/stream_error.hpp DESTINATION ${build_include_dir}/communication/bolt/v1/transport)
FILE(COPY ${include_dir}/communication/bolt/v1/packing/codes.hpp DESTINATION ${build_include_dir}/communication/bolt/v1/packing)
FILE(COPY ${include_dir}/communication/bolt/v1/messaging/codes.hpp DESTINATION ${build_include_dir}/communication/bolt/v1/messaging)

FILE(COPY ${include_dir}/io/network/socket.hpp DESTINATION ${build_include_dir}/io/network)
FILE(COPY ${include_dir}/io/network/addrinfo.hpp DESTINATION ${build_include_dir}/io/network)
FILE(COPY ${include_dir}/io/network/network_error.hpp DESTINATION ${build_include_dir}/io/network)
FILE(COPY ${include_dir}/io/network/socket.hpp DESTINATION ${build_include_dir}/io/network)

FILE(COPY ${include_dir}/logging/default.hpp DESTINATION ${build_include_dir}/logging)
FILE(COPY ${include_dir}/logging/log.hpp DESTINATION ${build_include_dir}/logging)
FILE(COPY ${include_dir}/logging/logger.hpp DESTINATION ${build_include_dir}/logging)
FILE(COPY ${include_dir}/logging/levels.hpp DESTINATION ${build_include_dir}/logging)
# -----------------------------------------------------------------------------

# add all cpp file recursive into sourceFiles varibale
# FILE(GLOB_RECURSE sourceFiles ${src_dir}/*.cpp)
# print list of source files
# MESSAGE(STATUS "All source files are: ${sourceFiles}")

# debug flags
if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    # set(CMAKE_CXX_FLAGS_DEBUG "-Wl,--export-dynamic ${CMAKE_CXX_FLAGS_DEBUG}")
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    # set(CMAKE_CXX_FLAGS_DEBUG "-rdynamic ${CMAKE_CXX_FLAGS_DEBUG}")
endif()

# release flags
set(CMAKE_CXX_FLAGS_RELEASE
    "${CMAKE_CXX_FLAGS_RELEASE}  -Wall")

# -- configure defines -- default is ON | true | enabled ----------------------
# -- logging ------------------------------------------------------------------
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
# -- logging ------------------------------------------------------------------
# -- logger -------------------------------------------------------------------
option(SYNC_LOGGER "" OFF)
message(STATUS "SYNC LOGGER: ${SYNC_LOGGER}")
if (SYNC_LOGGER)
    add_definitions(-DSYNC_LOGGER)
endif()
# -- logger -------------------------------------------------------------------
# -- assert -------------------------------------------------------------------
option(RUNTIME_ASSERT "Enable runtime assertions" ON)
message(STATUS "RUNTIME_ASSERT: ${RUNTIME_ASSERT}")
if(RUNTIME_ASSERT)
    add_definitions(-DRUNTIME_ASSERT_ON)
endif()

option(THROW_EXCEPTION_ON_ERROR "Throw exception on error" ON)
message(STATUS "THROW_EXCEPTION_ON_ERROR: ${THROW_EXCEPTION_ON_ERROR}")
if(THROW_EXCEPTION_ON_ERROR)
    add_definitions(-DTHROW_EXCEPTION_ON_ERROR)
endif()
# -- assert -------------------------------------------------------------------
# -- ndebug -------------------------------------------------------------------
option(NDEBUG "No debug" OFF)
message(STATUS "NDEBUG: ${NDEBUG} (be careful CMAKE_BUILD_TYPE can also append this flag)")
if(NDEBUG)
    add_definitions( -DNDEBUG )
endif()
# -- ndebug -------------------------------------------------------------------
# -- binaries -----------------------------------------------------------------
option(MEMGRAPH "Build memgraph binary" ON)
message(STATUS "MEMGRAPH binary: ${MEMGRAPH}")
option(POC "Build proof of concept binaries" ON)
message(STATUS "POC binaries: ${POC}")
option(TOOLS "Build tool executables" ON)
message(STATUS "TOOLS binaries: ${TOOLS}")
option(TESTS "Build test binaries" ON)
message(STATUS "TESTS binaries: ${TESTS}")
# -- binaries -----------------------------------------------------------------
# -- configure defines --------------------------------------------------------

# -- includes -----------------------------------------------------------------
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${src_dir})
include_directories(${build_include_dir})
include_directories(${fmt_source_dir})
include_directories(${http_parser_source_dir})
include_directories(${lexertl_dir})
include_directories(${libuv_source_dir}/include)
include_directories(${rapidjson_source_dir}/include)
include_directories(${r3_source_dir}/include)
# -----------------------------------------------------------------------------

# creates build/libcypher_lib.a
add_library(cypher_lib STATIC ${CMAKE_BINARY_DIR}/cypher.cpp)

# REST API preprocessor
EXECUTE_PROCESS(
    COMMAND python link_resources.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/api
)

# TODO: create separate static library from bolt code
set(memgraph_src_files
    ${src_dir}/utils/string/transform.cpp
    ${src_dir}/utils/string/join.cpp
    ${src_dir}/utils/string/file.cpp
    ${src_dir}/query_engine/util.cpp
    ${src_dir}/communication/bolt/v1/bolt.cpp
    ${src_dir}/communication/bolt/v1/states.cpp
    ${src_dir}/communication/bolt/v1/session.cpp
    ${src_dir}/communication/bolt/v1/states/error.cpp
    ${src_dir}/communication/bolt/v1/states/executor.cpp
    ${src_dir}/communication/bolt/v1/states/init.cpp
    ${src_dir}/communication/bolt/v1/states/handshake.cpp
    ${src_dir}/communication/bolt/v1/transport/bolt_decoder.cpp
    ${src_dir}/communication/bolt/v1/transport/buffer.cpp
    ${src_dir}/mvcc/id.cpp
    ${src_dir}/storage/vertices.cpp
    ${src_dir}/storage/edges.cpp
    ${src_dir}/storage/label/label.cpp
    ${src_dir}/storage/label/label_collection.cpp
    ${src_dir}/storage/label/label_store.cpp
    ${src_dir}/storage/edge_type/edge_type.cpp
    ${src_dir}/storage/edge_type/edge_type_store.cpp
    ${src_dir}/storage/model/properties/property.cpp
    ${src_dir}/storage/model/properties/null.cpp
    ${src_dir}/storage/model/properties/bool.cpp
    ${src_dir}/storage/model/properties/string.cpp
    ${src_dir}/storage/model/properties/array.cpp
    ${src_dir}/storage/model/properties/properties.cpp
    ${src_dir}/storage/model/properties/property_family.cpp
    ${src_dir}/storage/indexes/impl/nonunique_unordered_index.cpp
    ${src_dir}/storage/locking/record_lock.cpp
    ${src_dir}/storage/vertex_accessor.cpp
    ${src_dir}/transactions/transaction.cpp
    ${src_dir}/template_engine/engine.cpp
    ${src_dir}/logging/streams/stdout.cpp
    ${src_dir}/logging/levels.cpp
    ${src_dir}/logging/logs/sync_log.cpp
    ${src_dir}/logging/logs/async_log.cpp
    ${src_dir}/logging/default.cpp
    ${src_dir}/logging/log.cpp
    ${src_dir}/io/network/tls.cpp
    ${src_dir}/database/db.cpp
    ${src_dir}/database/db_accessor.cpp
    ${src_dir}/storage/edge_accessor.cpp
    ${src_dir}/storage/record_accessor.cpp
)

# STATIC library used by memgraph executables
add_library(memgraph STATIC ${memgraph_src_files})

# STATIC PIC library used by query engine
add_library(memgraph_pic STATIC ${memgraph_src_files})
set_property(TARGET memgraph_pic PROPERTY POSITION_INDEPENDENT_CODE TRUE)

# tests
if (TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# proof of concepts
if (POC)
    add_subdirectory(poc)
endif()


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
    "memgraph_${COMMIT_BRANCH}_${COMMIT_HASH}_${COMMIT_NO}_${CMAKE_BUILD_TYPE}")

# memgraph main executable
if (MEMGRAPH)
    add_executable(${MEMGRAPH_BUILD_NAME} ${src_dir}/memgraph_bolt.cpp)
    target_link_libraries(${MEMGRAPH_BUILD_NAME} memgraph)
    target_link_libraries(${MEMGRAPH_BUILD_NAME} Threads::Threads)
    target_link_libraries(${MEMGRAPH_BUILD_NAME} cypher_lib)
    if (UNIX)
        target_link_libraries(${MEMGRAPH_BUILD_NAME} crypto)
        # target_link_libraries(${MEMGRAPH_BUILD_NAME} ssl)
        target_link_libraries(${MEMGRAPH_BUILD_NAME} ${fmt_static_lib})
        target_link_libraries(${MEMGRAPH_BUILD_NAME} dl)
    endif (UNIX)
endif()

# # memgraph executable HTTP TODO: DEPRICATED
# add_executable(memgraph_http src/memgraph.cpp)
# add_dependencies(memgraph_http cypher_lib)
# target_link_libraries(memgraph_http Threads::Threads)
# target_link_libraries(memgraph_http pcre)
# target_link_libraries(memgraph_http ${libuv_static_lib})
# target_link_libraries(memgraph_http ${r3_static_lib})
# target_link_libraries(memgraph_http ${http_parser_static_lib})
