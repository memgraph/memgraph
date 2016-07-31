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
# glibcxx debug
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
# copy query_engine's templates file
FILE(COPY ${src_dir}/query_engine/template DESTINATION ${CMAKE_BINARY_DIR}/tests)
# create destination folder for compiled queries
FILE(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/tests/compiled/cpu)

# TODO: filter header files, all files don't need to be copied
# they are all copied because query engine needs header files during
# query compilation
# TODO: make a function (REMOVE copy pasted part)
SUBDIRLIST(source_folders ${src_dir})
foreach(source_folder ${source_folders})
    file(COPY ${src_dir}/${source_folder} DESTINATION ${build_include_dir})
endforeach()
SUBDIRLIST(source_folders ${src_dir})
foreach(source_folder ${source_folders})
    file(COPY ${src_dir}/${source_folder} DESTINATION ${test_src_dir})
endforeach()
SUBDIRLIST(source_folders ${include_dir})
foreach(source_foler ${source_folders})
    file(COPY ${include_dir}/${source_folder} DESTINATION ${test_include_dir})
endforeach()

# compiler options
SET(COMPILE_OPTIONS "-O2 -Wall -Werror -fmessage-length=0")

# add all cpp file recursive into sourceFiles varibale
# FILE(GLOB_RECURSE sourceFiles ${src_dir}/*.cpp)
# print list of source files
# MESSAGE(STATUS "All source files are: ${sourceFiles}")

# defines
option(RUNTIME_ASSERT "Enable runtime assertions" OFF)
if(RUNTIME_ASSERT)
    add_definitions( -DRUNTIME_ASSERT_ON )
endif()

option(THROW_EXCEPTION_ON_ERROR "Throw exception on error" OFF)
if(THROW_EXCEPTION_ON_ERROR)
    add_definitions( -DTHROW_EXCEPTION_ON_ERROR )
endif()

option(NDEBUG "No debug" OFF)
if(NDEBUG)
    add_definitions( -DNDEBUG )
endif()

# includes
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${src_dir})
include_directories(${build_include_dir})
include_directories(${fmt_source_dir})
include_directories(${http_parser_source_dir})
include_directories(${lexertl_dir})
include_directories(${libuv_source_dir}/include)
include_directories(${rapidjson_source_dir}/include)
include_directories(${r3_source_dir}/include)

# creates build/libcypher_lib.a
add_library(cypher_lib STATIC ${CMAKE_BINARY_DIR}/cypher.cpp)

# REST API preprocessor
EXECUTE_PROCESS(
    COMMAND python link_resources.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/api
)

# # memgraph executable
# add_executable(memgraph_http src/memgraph.cpp)
# add_dependencies(memgraph_http cypher_lib)
# target_link_libraries(memgraph_http Threads::Threads)
# target_link_libraries(memgraph_http pcre)
# target_link_libraries(memgraph_http ${libuv_static_lib})
# target_link_libraries(memgraph_http ${r3_static_lib})
# target_link_libraries(memgraph_http ${http_parser_static_lib})

# # query_engine executable
# add_executable(query_engine src/query_engine/main_query_engine.cpp)
# # query_engine link libraries
# target_link_libraries(query_engine Threads::Threads)
# target_link_libraries(query_engine dl)
# target_link_libraries(query_engine cypher_lib)
# target_link_libraries(query_engine ${fmt_static_lib})

# # query hasher executable
# add_executable(query_hasher src/query_engine/main_query_hasher.cpp)
# target_link_libraries(query_hasher ${fmt_static_lib})

set(memgraph_src_files
    ${src_dir}/mvcc/id.cpp
    ${src_dir}/storage/vertices.cpp
    ${src_dir}/storage/label/label.cpp
    ${src_dir}/storage/label/label_collection.cpp
    ${src_dir}/storage/label/label_store.cpp
    ${src_dir}/storage/edge_type/edge_type.cpp
    ${src_dir}/storage/edge_type/edge_type_store.cpp
    ${src_dir}/storage/model/properties/property.cpp
    ${src_dir}/storage/model/properties/null.cpp
    ${src_dir}/storage/model/properties/bool.cpp
    ${src_dir}/storage/model/properties/string.cpp
    ${src_dir}/storage/model/properties/properties.cpp
    ${src_dir}/storage/locking/record_lock.cpp
    ${src_dir}/storage/vertex_accessor.cpp
    ${src_dir}/transactions/transaction.cpp
    ${src_dir}/template_engine/engine.cpp
)
add_library(libmemgraph ${memgraph_src_files})

# tests
enable_testing()
add_subdirectory(tests)

# memgraph build name
execute_process(
    OUTPUT_VARIABLE COMMIT_NO 
    COMMAND git rev-list --count HEAD
)
execute_process(
    OUTPUT_VARIABLE COMMIT_HASH
    COMMAND git rev-parse --short HEAD
)
string(STRIP ${COMMIT_HASH} COMMIT_HASH)
string(STRIP ${COMMIT_NO} COMMIT_NO)
set(MEMGRAPH_BUILD_NAME "memgraph_${COMMIT_HASH}_${COMMIT_NO}")

# DEBUG BUILD
add_executable(${MEMGRAPH_BUILD_NAME}_debug ${src_dir}/memgraph_bolt.cpp)

# TEST BUILD
# TODO

# O-OPTIMIZED BUILD
# TODO

# RELEASE BUILD
# TODO
