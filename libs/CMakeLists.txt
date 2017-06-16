cmake_minimum_required(VERSION 3.1)

# setup antlr
option(WITH_LIBCXX "" OFF) # because of debian bug
# http://stackoverflow.com/questions/37096062/get-a-basic-c-program-to-compile-using-clang-on-ubuntu-16/38385967#38385967
add_subdirectory(antlr4/runtime/Cpp)

# setup google benchmark
add_subdirectory(benchmark)

# setup fmt format
# fmt uses google test but if fmt isn't top project (here it isn't) fmt tests
# are disabled (reasonable configuration)
add_subdirectory(fmt)

# setup google test
add_subdirectory(googletest)

# setup google flags
set(GFLAGS_BUILD_gflags_nothreads_LIB OFF)
set(GFLAGS_BUILD_gflags_LIB ON)
add_subdirectory(gflags)

# Setup google logging after gflags (so that glog can use it).
# We need to use `ExternalProject`, because currently glog's CMakeLists.txt
# doesn't detect gflags if we do `add_subdirectory`.
include(ExternalProject)
ExternalProject_Add(glog DEPENDS gflags
  PREFIX ${CMAKE_SOURCE_DIR}/libs/glog
  SOURCE_DIR ${CMAKE_SOURCE_DIR}/libs/glog
  CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release
             -DCMAKE_INSTALL_PREFIX=${CMAKE_SOURCE_DIR}/libs/glog
             -Dgflags_DIR=${CMAKE_CURRENT_BINARY_DIR}/gflags)

# setup cppitertools
# CLion compatiblity; the target won't be built
file(GLOB __CPPITERTOOLS_SOURCES __main.cpp
                                 ${CMAKE_SOURCE_DIR}/libs/cppitertools/*.hpp)
add_executable(__cppitertools_target ${__CPPITERTOOLS_SOURCES})
set_target_properties(__cppitertools_target PROPERTIES EXCLUDE_FROM_ALL 1)
