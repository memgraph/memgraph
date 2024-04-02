include(ExternalProject)

include(GNUInstallDirs)

include(ProcessorCount)
ProcessorCount(NPROC)

if(NPROC EQUAL 0)
  set(NPROC 1)
endif()

find_package(Boost 1.78 REQUIRED)
find_package(BZip2 1.0.6 REQUIRED)
find_package(Threads REQUIRED)
set(GFLAGS_NOTHREADS OFF)

# NOTE: config/generate.py depends on the gflags help XML format.
find_package(gflags REQUIRED)
find_package(fmt 8.0.1 REQUIRED)
find_package(ZLIB 1.2.11 REQUIRED)

set(LIB_DIR ${CMAKE_CURRENT_SOURCE_DIR})

# An uncommon pattern we can safely use as list separator in ExternalProject
set(LIST_SEP "-*-")

# convenience functions
function(import_header_library name include_dir)
  add_library(${name} INTERFACE IMPORTED GLOBAL)
  set_property(TARGET ${name} PROPERTY
    INTERFACE_INCLUDE_DIRECTORIES ${include_dir})
  string(TOUPPER ${name} _upper_name)
  set(${_upper_name}_INCLUDE_DIR ${include_dir} CACHE FILEPATH
    "Path to ${name} include directory" FORCE)
  mark_as_advanced(${_upper_name}_INCLUDE_DIR)
  add_library(lib::${name} ALIAS ${name})
endfunction(import_header_library)

function(import_library name type location include_dir)
  add_library(${name} ${type} IMPORTED GLOBAL)
  # https://cmake.org/cmake/help/latest/command/function.html#arguments
  if(ARGN)
    # Optional argument is the name of the external project that we need to
    # depend on.
    list(GET ARGN 0 dependency_name)
    add_dependencies(${name} ${dependency_name})
    message(STATUS "ADDED DEPENDENCY ${name} -> ${dependency_name}")
  else()
    add_dependencies(${name} ${name}-proj)
    message(STATUS "ADDED DEPENDENCY ${name} -> ${name}-proj")
  endif()
  set_property(TARGET ${name} PROPERTY IMPORTED_LOCATION ${location})
  # We need to create the include directory first in order to be able to add it
  # as an include directory. The header files in the include directory will be
  # generated later during the build process.
  file(MAKE_DIRECTORY ${include_dir})
  target_include_directories(${name} INTERFACE ${include_dir})
endfunction(import_library)

macro(add_external_project_arg_parse pos prefix)
  set(options NO_C_COMPILER)
  set(one_value_kwargs SOURCE_DIR BUILD_IN_SOURCE CMAKE_PREFIX_PATH)
  set(multi_value_kwargs CMAKE_ARGS DEPENDS INSTALL_COMMAND BUILD_COMMAND
          CONFIGURE_COMMAND BUILD_BYPRODUCTS)
  cmake_parse_arguments(PARSE_ARGV ${pos} "${prefix}" "${options}" "${one_value_kwargs}" "${multi_value_kwargs}")
endmacro()

macro(add_external_project_impl name)
  set(source_dir ${CMAKE_CURRENT_SOURCE_DIR}/${name})

  if(KW_SOURCE_DIR)
    set(source_dir ${KW_SOURCE_DIR})
  endif()

  set(build_in_source 0)

  if(KW_BUILD_IN_SOURCE)
    set(build_in_source ${KW_BUILD_IN_SOURCE})
  endif()

  if(NOT KW_NO_C_COMPILER)
    set(KW_CMAKE_ARGS -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} ${KW_CMAKE_ARGS})
  endif()

  # Add MG_TOOLCHAIN_ROOT to KW_CMAKE_PREFIX_PATH
  if(KW_CMAKE_PREFIX_PATH)
    set(KW_CMAKE_PREFIX_PATH "${MG_TOOLCHAIN_ROOT}${LIST_SEP}${KW_CMAKE_PREFIX_PATH}")
  else ()
    set(KW_CMAKE_PREFIX_PATH "${MG_TOOLCHAIN_ROOT}")
  endif ()

  ExternalProject_Add(${name}-proj DEPENDS ${KW_DEPENDS}
      PREFIX ${source_dir} SOURCE_DIR ${source_dir}
      BUILD_IN_SOURCE ${build_in_source}
      LIST_SEPARATOR "${LIST_SEP}"
      CONFIGURE_COMMAND ${KW_CONFIGURE_COMMAND}
      CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release
      -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
      -DCMAKE_INSTALL_PREFIX=${source_dir}
      $<$<BOOL:KW_CMAKE_PREFIX_PATH>:"-DCMAKE_PREFIX_PATH=${KW_CMAKE_PREFIX_PATH}">
      ${KW_CMAKE_ARGS}
      INSTALL_COMMAND ${KW_INSTALL_COMMAND}
      BUILD_COMMAND ${KW_BUILD_COMMAND}
      BUILD_BYPRODUCTS ${KW_BUILD_BYPRODUCTS})
endmacro()

# Calls `ExternalProject_Add(${name}-proj` with default arguments for cmake
# configuration. CMAKE_BUILD_TYPE is set to Release, CMAKE_C_COMPILER and
# CMAKE_CXX_COMPILER are forwarded as used in this project. You can pass
# NO_C_COMPILER option to avoid forwarding CMAKE_C_COMPILER. Installation is
# done in SOURCE_DIR, which defaults to ${CMAKE_CURRENT_SOURCE_DIR}/${name}.
# You can pass additional arguments via CMAKE_ARGS. Dependencies and
# installation can be set as in regular ExternalProject_Add, via DEPENDS and
# INSTALL_COMMAND arguments. It's also possible to pass BUILD_BYPRODUCTS.
function(add_external_project name)
  add_external_project_arg_parse(1 KW)
  add_external_project_impl(${name})
endfunction(add_external_project)

# Calls `add_external_project`, sets NAME_LIBRARY, NAME_INCLUDE_DIR variables
# and adds the library via `import_library`.
function(import_external_library name type library_location include_dir)
  add_external_project_arg_parse(4 KW)
  add_external_project_impl(${name})
  string(TOUPPER ${name} _upper_name)
  set(${_upper_name}_LIBRARY ${library_location} CACHE FILEPATH
    "Path to ${name} library" FORCE)
  set(${_upper_name}_INCLUDE_DIR ${include_dir} CACHE FILEPATH
    "Path to ${name} include directory" FORCE)
  mark_as_advanced(${_upper_name}_LIBRARY ${_upper_name}_INCLUDE_DIR)
  import_library(${name} ${type} ${${_upper_name}_LIBRARY} ${${_upper_name}_INCLUDE_DIR})
endfunction(import_external_library)


macro(set_path_external_library name type library_location include_dir)
  string(TOUPPER ${name} _upper_name)
  set(${_upper_name}_LIBRARY ${library_location} CACHE FILEPATH
      "Path to ${name} library" FORCE)
  set(${_upper_name}_INCLUDE_DIR ${include_dir} CACHE FILEPATH
      "Path to ${name} include directory" FORCE)
  mark_as_advanced(${name}_LIBRARY ${name}_INCLUDE_DIR)
endmacro(set_path_external_library)


# setup antlr
import_external_library(antlr4 STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/antlr4/runtime/Cpp/lib/libantlr4-runtime.a
  ${CMAKE_CURRENT_SOURCE_DIR}/antlr4/runtime/Cpp/include/antlr4-runtime
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/antlr4/runtime/Cpp
  CMAKE_ARGS # http://stackoverflow.com/questions/37096062/get-a-basic-c-program-to-compile-using-clang-on-ubuntu-16/38385967#38385967
  -DWITH_LIBCXX=OFF # because of debian bug
  -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=true
  -DCMAKE_CXX_STANDARD=20
  -DANTLR_BUILD_CPP_TESTS=OFF
  BUILD_COMMAND $(MAKE) antlr4_static
  INSTALL_COMMAND $(MAKE) install)

# Setup google benchmark.
import_external_library(benchmark STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/benchmark/${CMAKE_INSTALL_LIBDIR}/libbenchmark.a
  ${CMAKE_CURRENT_SOURCE_DIR}/benchmark/include
  # Skip testing. The tests don't compile with Clang 8.
  CMAKE_ARGS -DBENCHMARK_ENABLE_TESTING=OFF)

include(FetchContent)

# setup rapidcheck (it cannot be external, since it doesn't have install
# target)
set(RC_ENABLE_GTEST ON CACHE BOOL "Build Google Test integration" FORCE)
set(RC_ENABLE_GMOCK ON CACHE BOOL "Build Google Mock integration" FORCE)
mark_as_advanced(RC_ENABLE_GTEST RC_ENABLE_GMOCK)
add_subdirectory(rapidcheck EXCLUDE_FROM_ALL)

# setup google test
add_external_project(gtest SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/googletest
  CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR=lib)
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
import_library(gtest STATIC ${GTEST_LIBRARY} ${GTEST_INCLUDE_DIR} gtest-proj)
import_library(gtest_main STATIC ${GTEST_MAIN_LIBRARY} ${GTEST_INCLUDE_DIR} gtest-proj)
import_library(gmock STATIC ${GMOCK_LIBRARY} ${GTEST_INCLUDE_DIR} gtest-proj)
import_library(gmock_main STATIC ${GMOCK_MAIN_LIBRARY} ${GTEST_INCLUDE_DIR} gtest-proj)

# Setup cppitertools
import_header_library(cppitertools ${CMAKE_CURRENT_SOURCE_DIR})

# Setup json
import_header_library(json ${CMAKE_CURRENT_SOURCE_DIR})

# Setup RocksDB
import_external_library(rocksdb STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/rocksdb/lib/librocksdb.a
  ${CMAKE_CURRENT_SOURCE_DIR}/rocksdb/include
  CMAKE_ARGS -DUSE_RTTI=ON
  -DWITH_TESTS=OFF
  -DGFLAGS_NOTHREADS=OFF
  -DCMAKE_INSTALL_LIBDIR=lib
  -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=true
  BUILD_COMMAND $(MAKE) rocksdb)

# Setup libbcrypt
import_external_library(libbcrypt STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/libbcrypt/bcrypt.a
  ${CMAKE_CURRENT_SOURCE_DIR}/libbcrypt
  CONFIGURE_COMMAND sed s/-Wcast-align// -i ${CMAKE_CURRENT_SOURCE_DIR}/libbcrypt/crypt_blowfish/Makefile
  BUILD_COMMAND make -C ${CMAKE_CURRENT_SOURCE_DIR}/libbcrypt
  CC=${CMAKE_C_COMPILER}
  CXX=${CMAKE_CXX_COMPILER}
  INSTALL_COMMAND true)

# Setup mgclient
import_external_library(mgclient STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/mgclient/lib/libmgclient.a
  ${CMAKE_CURRENT_SOURCE_DIR}/mgclient/include
  CMAKE_ARGS -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
  -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
  -DBUILD_TESTING=OFF
  -DBUILD_CPP_BINDINGS=ON)
find_package(OpenSSL REQUIRED)
target_link_libraries(mgclient INTERFACE ${OPENSSL_LIBRARIES})

add_external_project(mgconsole
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/mgconsole
  CMAKE_ARGS
  -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_BINARY_DIR}
  BUILD_COMMAND $(MAKE) mgconsole)

add_custom_target(mgconsole DEPENDS mgconsole-proj)

# Setup spdlog
set(SPDLOG_FMT_EXTERNAL ON)
FetchContent_Declare(spdlog
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/spdlog)

FetchContent_MakeAvailable(spdlog)

# Setup librdkafka.
import_external_library(librdkafka STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/librdkafka/lib/librdkafka.a
  ${CMAKE_CURRENT_SOURCE_DIR}/librdkafka/include
  CMAKE_ARGS -DRDKAFKA_BUILD_STATIC=ON
  -DRDKAFKA_BUILD_EXAMPLES=OFF
  -DRDKAFKA_BUILD_TESTS=OFF
  -DWITH_ZSTD=OFF
  -DENABLE_LZ4_EXT=OFF
  -DCMAKE_INSTALL_LIBDIR=lib
  -DWITH_SSL=ON

  # If we want SASL, we need to install it on build machines
  -DWITH_SASL=OFF)
target_link_libraries(librdkafka INTERFACE ${OPENSSL_LIBRARIES} ZLIB::ZLIB)

import_library(librdkafka++ STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/librdkafka/lib/librdkafka++.a
  ${CMAKE_CURRENT_SOURCE_DIR}/librdkafka/include
)
target_link_libraries(librdkafka++ INTERFACE librdkafka)

set(PROTOBUF_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/protobuf/lib)
import_external_library(protobuf STATIC
  ${PROTOBUF_ROOT}/lib/libprotobuf.a
  ${PROTOBUF_ROOT}/include
  BUILD_IN_SOURCE 1
  CONFIGURE_COMMAND true)

import_external_library(pulsar STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/pulsar/pulsar-client-cpp/lib/libpulsarwithdeps.a
  ${CMAKE_CURRENT_SOURCE_DIR}/pulsar/install/include
  BUILD_IN_SOURCE 1
  CONFIGURE_COMMAND cmake pulsar-client-cpp
  -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_SOURCE_DIR}/pulsar/install
  -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
  -DBUILD_DYNAMIC_LIB=OFF
  -DBUILD_STATIC_LIB=ON
  -DBUILD_TESTS=OFF
  -DLINK_STATIC=ON
  -DPROTOC_PATH=${PROTOBUF_ROOT}/bin/protoc
  -DBOOST_ROOT=${BOOST_ROOT}
  "-DCMAKE_PREFIX_PATH=$<$<BOOL:${MG_TOOLCHAIN_ROOT}>:${MG_TOOLCHAIN_ROOT}${LIST_SEP}>${PROTOBUF_ROOT}"
  -DProtobuf_INCLUDE_DIRS=${PROTOBUF_ROOT}/include
  -DBUILD_PYTHON_WRAPPER=OFF
  -DBUILD_PERF_TOOLS=OFF
  -DUSE_LOG4CXX=OFF
  BUILD_COMMAND $(MAKE) pulsarStaticWithDeps)
add_dependencies(pulsar-proj protobuf)

if(${MG_ARCH} STREQUAL "ARM64")
  set(MG_LIBRDTSC_CMAKE_ARGS -DLIBRDTSC_ARCH_x86=OFF -DLIBRDTSC_ARCH_ARM64=ON)
endif()

import_external_library(librdtsc STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/librdtsc/lib/librdtsc.a
  ${CMAKE_CURRENT_SOURCE_DIR}/librdtsc/include
  CMAKE_ARGS ${MG_LIBRDTSC_CMAKE_ARGS}
  BUILD_COMMAND $(MAKE) rdtsc)

# setup ctre
import_header_library(ctre ${CMAKE_CURRENT_SOURCE_DIR})

# setup absl (cmake sub_directory tolerant)
set(ABSL_PROPAGATE_CXX_STD ON)
add_subdirectory(absl EXCLUDE_FROM_ALL)

# set Jemalloc
set_path_external_library(jemalloc STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/jemalloc/lib/libjemalloc.a
  ${CMAKE_CURRENT_SOURCE_DIR}/jemalloc/include/)

import_header_library(rangev3 ${CMAKE_CURRENT_SOURCE_DIR}/rangev3/include)

# Setup NuRaft
import_external_library(nuraft STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/nuraft/lib/libnuraft.a
  ${CMAKE_CURRENT_SOURCE_DIR}/nuraft/include/)
find_package(OpenSSL REQUIRED)
target_link_libraries(nuraft INTERFACE ${OPENSSL_LIBRARIES})

# Setup mgcxx (provides: text search)
set(MGCXX_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/mgcxx/include
  CACHE PATH "Path to mgcxx include directory" FORCE)
set(TANTIVY_TEXT_SEARCH_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/mgcxx/lib/libtantivy_text_search.a
  CACHE FILEPATH "Path to mgcxx tantivy library (internal one)" FORCE)
set(MGCXX_TEXT_SEARCH_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/mgcxx/lib/libmgcxx_text_search.a
  CACHE FILEPATH "Path to mgcxx text search library" FORCE)
add_external_project(mgcxx
  SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/mgcxx
  CMAKE_ARGS -DENABLE_TESTS=OFF)
mark_as_advanced(MGCXX_INCLUDE_DIR TANTIVY_TEXT_SEARCH_LIBRARY MGCXX_TEXT_SEARCH_LIBRARY)
import_library(tantivy_text_search STATIC ${TANTIVY_TEXT_SEARCH_LIBRARY} ${MGCXX_INCLUDE_DIR} mgcxx-proj)
import_library(mgcxx_text_search STATIC ${MGCXX_TEXT_SEARCH_LIBRARY} ${MGCXX_INCLUDE_DIR} mgcxx-proj)
