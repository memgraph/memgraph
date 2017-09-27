# MemGraph Tools CMake configuration

cmake_minimum_required(VERSION 3.1)

if (NOT UNIX)
  message(FATAL, "Unsupported operating system.")
endif()

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
set(CMAKE_C_COMPILER "clang")
set(CMAKE_CXX_COMPILER "clang++")

project("memgraph_tools")

# setup CMake module path, defines path for include() and find_package()
# https://cmake.org/cmake/help/latest/variable/CMAKE_MODULE_PATH.html
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PROJECT_SOURCE_DIR}/../cmake)
# custom function definitions
include(functions)

disallow_in_source_build()

# threading
find_package(Threads REQUIRED)

# optional readline
find_package(Readline REQUIRED)
if (READLINE_FOUND)
  include_directories(SYSTEM ${READLINE_INCLUDE_DIR})
  add_definitions(-DHAS_READLINE)
endif()

# c++14
# TODO: set here 17 once it will be available in the cmake version (3.8)
set(cxx_standard 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1z -Wall -Wno-c++1z-extensions")
# Don't omit frame pointer in RelWithDebInfo, for additional callchain debug.
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO
    "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fno-omit-frame-pointer")

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

# Setup external dependencies. Use EXCLUDE_FROM_ALL to prevent *installing* libs.
add_subdirectory(${PROJECT_SOURCE_DIR}/../libs libs EXCLUDE_FROM_ALL)
include_directories(SYSTEM ${GFLAGS_INCLUDE_DIR})
include_directories(SYSTEM ${GLOG_INCLUDE_DIR})
include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/../libs)  # cppitertools
include_directories(SYSTEM ${FMT_INCLUDE_DIR})

# Include memgraph headers
set(memgraph_src_dir ${PROJECT_SOURCE_DIR}/../src)
include_directories(${memgraph_src_dir})

add_subdirectory(src)
