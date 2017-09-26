cmake_minimum_required(VERSION 3.1)
include(ExternalProject)

include(ProcessorCount)
ProcessorCount(NPROC)
if (NPROC EQUAL 0)
  set(NPROC 1)
endif()

# setup antlr
ExternalProject_Add(antlr4
  PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/antlr4/runtime/Cpp
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/antlr4/runtime/Cpp
  CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release
             -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
             -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
             -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_SOURCE_DIR}/antlr4/runtime/Cpp
             # http://stackoverflow.com/questions/37096062/get-a-basic-c-program-to-compile-using-clang-on-ubuntu-16/38385967#38385967
             -DWITH_LIBCXX=OFF # because of debian bug
  # Make a License.txt out of thin air, so that antlr4.6 knows how to build.
  # When we upgrade antlr, this will no longer be needed.
  INSTALL_COMMAND touch ${CMAKE_CURRENT_SOURCE_DIR}/antlr4/runtime/Cpp/License.txt
  COMMAND $(MAKE) install)
set(ANTLR4_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/antlr4/runtime/Cpp/lib/libantlr4-runtime.a
    CACHE FILEPATH "Path to antlr4 library" FORCE)
set(ANTLR4_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/antlr4/runtime/Cpp/include/antlr4-runtime
    CACHE PATH "Path to antlr4 include directory" FORCE)
mark_as_advanced(ANTLR4_LIBRARY ANTLR4_INCLUDE_DIR)

# setup google benchmark
add_subdirectory(benchmark EXCLUDE_FROM_ALL)

# setup fmt format
# fmt uses google test but if fmt isn't top project (here it isn't) fmt tests
# are disabled (reasonable configuration)
add_subdirectory(fmt EXCLUDE_FROM_ALL)

# setup rapidcheck
add_subdirectory(rapidcheck EXCLUDE_FROM_ALL)

# setup google test
add_subdirectory(googletest EXCLUDE_FROM_ALL)

# setup google flags
set(GFLAGS_BUILD_gflags_nothreads_LIB OFF)
set(GFLAGS_BUILD_gflags_LIB ON)
# Gflags has to be position independant otherwise Glog complains.
set(CMAKE_CXX_FLAGS_SAVED ${CMAKE_CXX_FLAGS})
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
add_subdirectory(gflags EXCLUDE_FROM_ALL)
set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS_SAVED})

# Setup google logging after gflags (so that glog can use it).
ExternalProject_Add(glog DEPENDS gflags
  PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/glog
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/glog
  CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release
             -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
             -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
             -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_SOURCE_DIR}/glog
             -Dgflags_DIR=${CMAKE_CURRENT_BINARY_DIR}/gflags)
set(GLOG_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/glog/lib/libglog.a CACHE FILEPATH
    "Path to glog library" FORCE)
set(GLOG_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/glog/include CACHE PATH
    "Path to glog include directory" FORCE)
mark_as_advanced(GLOG_LIBRARY GLOG_INCLUDE_DIR)

# setup cppitertools
# CLion compatiblity; the target won't be built
file(GLOB __CPPITERTOOLS_SOURCES __main.cpp
                                 ${CMAKE_SOURCE_DIR}/libs/cppitertools/*.hpp)
add_executable(__cppitertools_target ${__CPPITERTOOLS_SOURCES})
set_target_properties(__cppitertools_target PROPERTIES EXCLUDE_FROM_ALL 1)
