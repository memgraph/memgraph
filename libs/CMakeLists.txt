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

# Setup google benchmark.
ExternalProject_Add(benchmark
  PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/benchmark
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/benchmark
  CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release
             -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
             -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
             -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_SOURCE_DIR}/benchmark)
set(BENCHMARK_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/benchmark/lib/libbenchmark.a
    CACHE FILEPATH "Path to benchmark library" FORCE)
set(BENCHMARK_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/benchmark/include
    CACHE PATH "Path to benchmark include directory" FORCE)
mark_as_advanced(BENCHMARK_LIBRARY BENCHMARK_INCLUDE_DIR)

# setup fmt format
ExternalProject_Add(fmt
  PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/fmt
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/fmt
  CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release
             -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
             -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
             -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_SOURCE_DIR}/fmt
             # skip testing
             -DFMT_TEST=OFF)
set(FMT_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/fmt/lib/libfmt.a
    CACHE FILEPATH "Path to fmt library" FORCE)
set(FMT_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/fmt/include
    CACHE PATH "Path to fmt include directory" FORCE)
mark_as_advanced(FMT_LIBRARY FMT_INCLUDE_DIR)


# setup rapidcheck (it cannot be external, since it doesn't have install
# target)
add_subdirectory(rapidcheck EXCLUDE_FROM_ALL)

# setup google test
ExternalProject_Add(gtest
  PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/googletest
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/googletest
  CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release
             -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
             -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
             -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_SOURCE_DIR}/googletest)
set(GTEST_LIBRARIES
    ${CMAKE_CURRENT_SOURCE_DIR}/googletest/lib/libgmock.a
    ${CMAKE_CURRENT_SOURCE_DIR}/googletest/lib/libgmock_main.a
    ${CMAKE_CURRENT_SOURCE_DIR}/googletest/lib/libgtest.a
    ${CMAKE_CURRENT_SOURCE_DIR}/googletest/lib/libgtest_main.a
    CACHE FILEPATH "Path to googletest library" FORCE)
set(GTEST_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/googletest/include
    CACHE PATH "Path to googletest include directory" FORCE)
mark_as_advanced(GTEST_LIBRARIES GTEST_INCLUDE_DIR)

# setup google flags
set(GFLAGS_NO_FILENAMES "0")
if ("${CMAKE_BUILD_TYPE}" MATCHES "^(R|r)(E|e)(L|l).+")
  set(GFLAGS_NO_FILENAMES "1")
endif()

ExternalProject_Add(gflags
  PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/gflags
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/gflags
  CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release
             # Not needed, since gflags is C++ only.
             # -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
             -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
             -DGFLAGS_NO_FILENAMES=${GFLAGS_NO_FILENAMES}
             -DBUILD_gflags_nothreads_LIB=OFF
             -DREGISTER_INSTALL_PREFIX=OFF
             -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_SOURCE_DIR}/gflags)
set(GFLAGS_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/gflags/lib/libgflags.a
    CACHE FILEPATH "Path to gflags library" FORCE)
set(GFLAGS_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/gflags/include
    CACHE PATH "Path to gflags include directory" FORCE)
mark_as_advanced(GFLAGS_LIBRARY GFLAGS_INCLUDE_DIR)

# Setup google logging after gflags (so that glog can use it).
set(GLOG_NO_FILENAMES "0")
set(GLOG_NO_STACKTRACE "0")
if ("${CMAKE_BUILD_TYPE}" MATCHES "^(R|r)(E|e)(L|l).+")
  set(GLOG_NO_FILENAMES "1")
  set(GLOG_NO_STACKTRACE "1")
endif()

ExternalProject_Add(glog DEPENDS gflags
  PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/glog
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/glog
  CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release
             -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
             -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
             -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_SOURCE_DIR}/glog
             -DGLOG_NO_FILENAMES=${GLOG_NO_FILENAMES}
             -DGLOG_NO_STACKTRACE=${GLOG_NO_STACKTRACE}
             -Dgflags_DIR=${CMAKE_CURRENT_SOURCE_DIR}/gflags/lib/cmake/gflags)
set(GLOG_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/glog/lib/libglog.a CACHE FILEPATH
    "Path to glog library" FORCE)
set(GLOG_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/glog/include CACHE PATH
    "Path to glog include directory" FORCE)
mark_as_advanced(GLOG_LIBRARY GLOG_INCLUDE_DIR)
