cmake_minimum_required(VERSION 3.0)

# set directory name as the project name
# get directory name
get_filename_component(ProjectId ${CMAKE_SOURCE_DIR} NAME)
# replace whitespaces with underscores
string(REPLACE " " "_" ProjectId ${ProjectId})
# set project name
project(${ProjectId})

INCLUDE(ExternalProject)

# compiler options
SET(COMPILE_OPTIONS "-O0 -g3 -Wall -Werror -fmessage-length=0")

# add all cpp file recursive into sourceFiles varibale
FILE(GLOB_RECURSE sourceFiles ${CMAKE_HOME_DIRECTORY}/src/*.cpp)

# print list of source files
# MESSAGE(STATUS "All source files are: ${sourceFiles}")

INCLUDE_DIRECTORIES(${CMAKE_HOME_DIRECTORY}/src)
ENABLE_TESTING()
ADD_SUBDIRECTORY(tests)
