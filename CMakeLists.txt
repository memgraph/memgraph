cmake_minimum_required(VERSION 3.1)

# set directory name as the project name
# get directory name
get_filename_component(ProjectId ${CMAKE_SOURCE_DIR} NAME)
# replace whitespaces with underscores
string(REPLACE " " "_" ProjectId ${ProjectId})
# set project name
project(${ProjectId})

# c++14
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y")

# functions

function(list_includes)
    get_property(dirs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
    foreach(dir ${dirs})
          message(STATUS "dir='${dir}'")
    endforeach()
endfunction(list_includes)

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

include(ExternalProject)

set(build_include_dir ${CMAKE_BINARY_DIR}/include)
set(src_dir ${CMAKE_SOURCE_DIR}/src)
set(libs_dir ${CMAKE_SOURCE_DIR}/libs)

# lemon
set(lemon_dir "${libs_dir}/lemon")
set(lemon_tag "f38a55106d79b7a4c063abb958517d6c47dc6ac7")
set(lemon_url "http://www.sqlite.org/src/raw/tool/lemon.c?name=${lemon_tag}")
ExternalProject_Add(
    lemon
    DOWNLOAD_COMMAND wget ${lemon_url} -O lemon.c
    DOWNLOAD_DIR ${lemon_dir}
    SOURCE_DIR ${lemon_dir}
    BINARY_DIR ${lemon_dir}
	CONFIGURE_COMMAND ""
    BUILD_COMMAND clang lemon.c -o lemon -O2
    INSTALL_COMMAND ""
    TEST_COMMAND ""
)

# lempar
set(lempar_dir "${libs_dir}/lemon")
set(lempar_tag "404ea3dc27dbeed343f0e61b1d36e97b9f5f0fb6")
set(lempar_url "http://www.sqlite.org/src/raw/tool/lempar.c?name=${lempar_tag}")
ExternalProject_Add(
    lempar
    DOWNLOAD_COMMAND wget ${lempar_url} -O lempar.c
    DOWNLOAD_DIR ${lempar_dir}
    SOURCE_DIR ${lempar_dir}
    BINARY_DIR ${lempar_dir}
	CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
    TEST_COMMAND ""
)

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

# lexertl
set(lexertl_dir ${libs_dir}/lexertl)
ExternalProject_Add(
    lexertl
    GIT_REPOSITORY "https://github.com/BenHanson/lexertl.git"
    GIT_TAG "7d4d36a357027df0e817453cc9cf948f71047ca9"
    SOURCE_DIR "${libs_dir}/lexertl"
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
    TEST_COMMAND ""
)

# compiler options
SET(COMPILE_OPTIONS "-O0 -g3 -Wall -Werror -fmessage-length=0")

# add all cpp file recursive into sourceFiles varibale
FILE(GLOB_RECURSE sourceFiles ${src_dir}/*.cpp)

# print list of source files
# MESSAGE(STATUS "All source files are: ${sourceFiles}")

include_directories(${src_dir})
include_directories(${lexertl_dir})
include_directories(${build_include_dir})

# creates build/libcypher_lib.a
add_library(cypher_lib STATIC ${CMAKE_BINARY_DIR}/cypher.cpp)

enable_testing()
add_subdirectory(tests)
