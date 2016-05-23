cmake_minimum_required(VERSION 3.0)

# set directory name as the project name
# get directory name
get_filename_component(ProjectId ${CMAKE_SOURCE_DIR} NAME)
# replace whitespaces with underscores
string(REPLACE " " "_" ProjectId ${ProjectId})
# set project name
project(${ProjectId})

# external dependencies

include(ExternalProject)
set(libs_dir "${CMAKE_SOURCE_DIR}/libs")

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

# build memgraph's cypher grammer
FILE(COPY ${CMAKE_SOURCE_DIR}/src/cypher/cypher.y DESTINATION ${CMAKE_BINARY_DIR})
EXECUTE_PROCESS(
    COMMAND ${lemon_dir}/lemon ${CMAKE_BINARY_DIR}/cypher.y -s
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)
FILE(RENAME ${CMAKE_BINARY_DIR}/cypher.c ${CMAKE_BINARY_DIR}/cypher.cpp)

# lexertl
ExternalProject_Add(
    lexertl
    GIT_REPOSITORY "https://github.com/BenHanson/lexertl.git"
    GIT_TAG "7d4d36a357027df0e817453cc9cf948f71047ca9"
    SOURCE_DIR "${libs_dir}/lexertl"
    TEST_COMMAND ""
    INSTALL_COMMAND ""
)

# compiler options
SET(COMPILE_OPTIONS "-O0 -g3 -Wall -Werror -fmessage-length=0")

# add all cpp file recursive into sourceFiles varibale
FILE(GLOB_RECURSE sourceFiles ${CMAKE_HOME_DIRECTORY}/src/*.cpp)

# print list of source files
# MESSAGE(STATUS "All source files are: ${sourceFiles}")

INCLUDE_DIRECTORIES(${CMAKE_HOME_DIRECTORY}/src)
ENABLE_TESTING()
ADD_SUBDIRECTORY(tests)
