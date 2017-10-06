cmake_minimum_required(VERSION 3.1)
include(ExternalProject)

include(ProcessorCount)
ProcessorCount(NPROC)
if (NPROC EQUAL 0)
  set(NPROC 1)
endif()

# convenience functions
function(import_header_library name include_dir)
  add_library(${name} INTERFACE IMPORTED GLOBAL)
  set_property(TARGET ${name} PROPERTY
               INTERFACE_INCLUDE_DIRECTORIES ${include_dir})
endfunction(import_header_library)

function(import_library name type location)
  add_library(${name} ${type} IMPORTED GLOBAL)
  if (${ARGN})
    # Optional argument is the name of the external project that we need to
    # depend on.
    add_dependencies(${name} ${ARGN0})
  else()
    add_dependencies(${name} ${name}-proj)
  endif()
  # Unfortunately, we cannot use include_dir before it is built.
  # set_property(TARGET ${name} PROPERTY
  #              INTERFACE_INCLUDE_DIRECTORIES ${include_dir})
  set_property(TARGET ${name} PROPERTY IMPORTED_LOCATION ${location})
endfunction(import_library)

# Calls `ExternalProject_Add(${name}-proj` with default arguments for cmake
# configuration. CMAKE_BUILD_TYPE is set to Release, CMAKE_C_COMPILER and
# CMAKE_CXX_COMPILER are forwarded as used in this project. You can pass
# NO_C_COMPILER option to avoid forwarding CMAKE_C_COMPILER. Installation is
# done in SOURCE_DIR, which defaults to ${CMAKE_CURRENT_SOURCE_DIR}/${name}.
# You can pass additional arguments via CMAKE_ARGS. Dependencies and
# installation can be set as in regular ExternalProject_Add, via DEPENDS and
# INSTALL_COMMAND arguments.
function(add_external_project name)
  set(options NO_C_COMPILER)
  set(one_value_kwargs SOURCE_DIR)
  set(multi_value_kwargs CMAKE_ARGS DEPENDS INSTALL_COMMAND)
  cmake_parse_arguments(KW "${options}" "${one_value_kwargs}" "${multi_value_kwargs}" ${ARGN})
  set(source_dir ${CMAKE_CURRENT_SOURCE_DIR}/${name})
  if (KW_SOURCE_DIR)
    set(source_dir ${KW_SOURCE_DIR})
  endif()
  if (NOT KW_NO_C_COMPILER)
    set(KW_CMAKE_ARGS -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} ${KW_CMAKE_ARGS})
  endif()
  ExternalProject_Add(${name}-proj DEPENDS ${KW_DEPENDS}
                      PREFIX ${source_dir} SOURCE_DIR ${source_dir}
                      CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release
                                 -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
                                 -DCMAKE_INSTALL_PREFIX=${source_dir}
                                 ${KW_CMAKE_ARGS}
                      INSTALL_COMMAND ${KW_INSTALL_COMMAND})
  # Force the build step (which will check if the source changed) by removing
  # the stamped build file.
  ExternalProject_Add_Step(
    ${name}-proj force-build
    COMMAND ${CMAKE_COMMAND} -E remove ${CMAKE_CURRENT_BINARY_DIR}/src/${name}-proj-stamp/${name}-proj-build
    DEPENDERS configure ALWAYS 1)
endfunction(add_external_project)

# Calls `add_external_project`, sets NAME_LIBRARY, NAME_INCLUDE_DIR variables
# and adds the library via `import_library`.
macro(import_external_library name type library_location include_dir)
  add_external_project(${name} ${ARGN})
  string(TOUPPER ${name} _upper_name)
  set(${_upper_name}_LIBRARY ${library_location} CACHE FILEPATH
      "Path to ${name} library" FORCE)
  set(${_upper_name}_INCLUDE_DIR ${include_dir} CACHE FILEPATH
      "Path to ${name} include directory" FORCE)
  mark_as_advanced(${_upper_name}_LIBRARY ${_upper_name}_INCLUDE_DIR)
  import_library(${name} ${type} ${${_upper_name}_LIBRARY})
endmacro(import_external_library)

# setup antlr
import_external_library(antlr4 STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/antlr4/runtime/Cpp/lib/libantlr4-runtime.a
  ${CMAKE_CURRENT_SOURCE_DIR}/antlr4/runtime/Cpp/include/antlr4-runtime
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/antlr4/runtime/Cpp
  CMAKE_ARGS # http://stackoverflow.com/questions/37096062/get-a-basic-c-program-to-compile-using-clang-on-ubuntu-16/38385967#38385967
             -DWITH_LIBCXX=OFF # because of debian bug
  # Make a License.txt out of thin air, so that antlr4.6 knows how to build.
  # When we upgrade antlr, this will no longer be needed.
  INSTALL_COMMAND touch ${CMAKE_CURRENT_SOURCE_DIR}/antlr4/runtime/Cpp/License.txt
  COMMAND $(MAKE) install)

# Setup google benchmark.
import_external_library(benchmark STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/benchmark/lib/libbenchmark.a
  ${CMAKE_CURRENT_SOURCE_DIR}/benchmark/include)

# setup fmt format
import_external_library(fmt STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/fmt/lib/libfmt.a
  ${CMAKE_CURRENT_SOURCE_DIR}/fmt/include
  # Skip testing.
  CMAKE_ARGS -DFMT_TEST=OFF)

# setup rapidcheck (it cannot be external, since it doesn't have install
# target)
set(RC_ENABLE_GTEST ON CACHE BOOL "Build Google Test integration" FORCE)
set(RC_ENABLE_GMOCK ON CACHE BOOL "Build Google Mock integration" FORCE)
mark_as_advanced(RC_ENABLE_GTEST RC_ENABLE_GMOCK)
add_subdirectory(rapidcheck EXCLUDE_FROM_ALL)

# setup google test
add_external_project(gtest SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/googletest)
set(GTEST_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/googletest/include
    CACHE PATH "Path to gtest and gmock include directory" FORCE)
set(GMOCK_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/googletest/lib/libgmock.a
    CACHE FILEPATH "Path to gmock library" FORCE)
set(GMOCK_MAIN_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/googletest/lib/libgmock_main.a
    CACHE FILEPATH "Path to gmock_main library" FORCE)
set(GTEST_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/googletest/lib/libgtest.a
    CACHE FILEPATH "Path to gtest library" FORCE)
set(GTEST_MAIN_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/googletest/lib/libgtest_main.a
    CACHE FILEPATH "Path to gtest_main library" FORCE)
mark_as_advanced(GTEST_INCLUDE_DIR GMOCK_LIBRARY GMOCK_MAIN_LIBRARY GTEST_LIBRARY GTEST_MAIN_LIBRARY)
import_library(gtest STATIC ${GTEST_LIBRARY} gtest-proj)
import_library(gtest_main STATIC ${GTEST_MAIN_LIBRARY} gtest-proj)
import_library(gmock STATIC ${GMOCK_LIBRARY} gtest-proj)
import_library(gmock_main STATIC ${GMOCK_MAIN_LIBRARY} gtest-proj)

# setup google flags
set(GFLAGS_NO_FILENAMES "0")
if ("${CMAKE_BUILD_TYPE}" MATCHES "^(R|r)(E|e)(L|l).+")
  set(GFLAGS_NO_FILENAMES "1")
endif()

# setup google flags
import_external_library(gflags STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/gflags/lib/libgflags.a
  ${CMAKE_CURRENT_SOURCE_DIR}/gflags/include
  # Not needed, since gflags is C++ only.
  NO_C_COMPILER
  # Don't register installation in ~/.cmake
  CMAKE_ARGS -DREGISTER_INSTALL_PREFIX=OFF
             -DBUILD_gflags_nothreads_LIB=OFF
             -DGFLAGS_NO_FILENAMES=${GFLAGS_NO_FILENAMES})

# Setup google logging after gflags (so that glog can use it).
set(GLOG_DISABLE_OPTIONS "0")
if ("${CMAKE_BUILD_TYPE}" MATCHES "^(R|r)(E|e)(L|l).+")
  set(GLOG_DISABLE_OPTIONS "1")
endif()

# Setup google logging after gflags (so that glog can use it).
import_external_library(glog STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/glog/lib/libglog.a
  ${CMAKE_CURRENT_SOURCE_DIR}/glog/include
  DEPENDS gflags-proj
  CMAKE_ARGS -Dgflags_DIR=${CMAKE_CURRENT_SOURCE_DIR}/gflags/lib/cmake/gflags
             -DGLOG_NO_FILENAMES=${GLOG_DISABLE_OPTIONS}
             -DGLOG_NO_STACKTRACE=${GLOG_DISABLE_OPTIONS}
             -DGLOG_NO_BUFFER_SETTINGS=${GLOG_DISABLE_OPTIONS})

# Setup cppitertools
import_header_library(cppitertools ${CMAKE_CURRENT_SOURCE_DIR})

# Setup json
import_header_library(json ${CMAKE_CURRENT_SOURCE_DIR})
