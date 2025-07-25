include(ExternalProject)
include(FetchContent)
include(GNUInstallDirs)

include(ProcessorCount)
ProcessorCount(NPROC)

if(NPROC EQUAL 0)
  set(NPROC 1)
endif()

find_package(Boost 1.78 REQUIRED CONFIG)
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

include(FetchContent)

# NOTE: A lot of the below code is just a tmp solution to rollout toolchain-v6.
# All code depending on the toolchain version should be removed and replaced
# with the code to load library from: 1) the system, 2) the toolchain, 3) build
# custom version if wanted.

# NOTE: Called under, e.g., src/kvstore/CMakeLists.txt
macro(mg_target_link_library_cmake toolchain_version mg_target_name package_name toolchain_lib_name local_lib_name)
  if(DEFINED ENV{MG_TOOLCHAIN_VERSION})
    if($ENV{MG_TOOLCHAIN_VERSION} GREATER_EQUAL ${toolchain_version})
      message(STATUS "TOOLCHAIN v${toolchain_version}")
      find_package(${package_name} REQUIRED)
      target_link_libraries(${mg_target_name} ${toolchain_lib_name})
    else()
      message(STATUS "Not the right TOOLCHAIN")
      target_link_libraries(${mg_target_name} ${local_lib_name})
    endif()
  else()
    message(STATUS "Unknown TOOLCHAIN")
    target_link_libraries(${mg_target_name} ${local_lib_name})
  endif()
endmacro()

macro(mg_build_if_needed build_macro lib_name)
  if(DEFINED ENV{MG_TOOLCHAIN_VERSION})
    if($ENV{MG_TOOLCHAIN_VERSION} GREATER_EQUAL 6)
      message(STATUS "NOTE: ${lib_name} should be under the toolchain.")
    else()
      cmake_language(CALL ${build_macro})
    endif()
  else()
    cmake_language(CALL ${build_macro})
  endif()
endmacro()

macro(mg_create_linkable_if_no_cmake lib_name lib_type lib_path include_path)
  if(DEFINED ENV{MG_TOOLCHAIN_VERSION})
    if($ENV{MG_TOOLCHAIN_VERSION} GREATER_EQUAL 6)
      # NOTE: This is like the import_library, but the existing code
      # couldn't be used because it depends on the building the
      # project(lib) locally...
      add_library(${lib_name} ${lib_type} IMPORTED GLOBAL)
      set_property(TARGET ${lib_name} PROPERTY IMPORTED_LOCATION ${lib_path})
      # We need to create the include directory first in order to be able to add it
      # as an include directory. The header files in the include directory will be
      # generated later during the build process.
      file(MAKE_DIRECTORY ${include_path})
      target_include_directories(${lib_name} INTERFACE ${include_path})
      message(STATUS "NOTE: Imported ${lib_name} from ${lib_path} and ${include_path}")
    else()
      message(STATUS "NOTE: ${lib_name} should be built locally.")
    endif()
  else()
    message(STATUS "NOTE: ${lib_name} should be built locally.")
  endif()
endmacro()

macro(build_antlr4)
  import_external_library(antlr4 STATIC
    ${CMAKE_CURRENT_SOURCE_DIR}/antlr4/runtime/Cpp/lib/libantlr4-runtime.a
    ${CMAKE_CURRENT_SOURCE_DIR}/antlr4/runtime/Cpp/include/antlr4-runtime
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/antlr4/runtime/Cpp
    CMAKE_ARGS # http://stackoverflow.com/questions/37096062/get-a-basic-c-program-to-compile-using-clang-on-ubuntu-16/38385967#38385967
    -DWITH_LIBCXX=OFF # because of debian bug
    -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=true
    -DCMAKE_CXX_STANDARD=20
    -DANTLR_BUILD_CPP_TESTS=OFF
    -DANTLR_BUILD_SHARED=OFF
    BUILD_COMMAND $(MAKE) antlr4_static
    INSTALL_COMMAND $(MAKE) install)
endmacro()
mg_build_if_needed(build_antlr4 "antlr4")
mg_create_linkable_if_no_cmake(antlr4 STATIC
  "${MG_TOOLCHAIN_ROOT}/lib/libantlr4-runtime.a"
  "${MG_TOOLCHAIN_ROOT}/include/antlr4-runtime"
)

macro(build_gbenchmark)
  import_external_library(benchmark STATIC
    ${CMAKE_CURRENT_SOURCE_DIR}/benchmark/${CMAKE_INSTALL_LIBDIR}/libbenchmark.a
    ${CMAKE_CURRENT_SOURCE_DIR}/benchmark/include
    # Skip testing. The tests don't compile with Clang 8.
    CMAKE_ARGS -DBENCHMARK_ENABLE_TESTING=OFF)
endmacro()
mg_build_if_needed(build_gbenchmark "gbenchmark")
mg_create_linkable_if_no_cmake(benchmark STATIC
  "${MG_TOOLCHAIN_ROOT}/lib/libbenchmark.a"
  "${MG_TOOLCHAIN_ROOT}/include"
)

# setup rapidcheck (it cannot be external, since it doesn't have install
# target)
set(RC_ENABLE_GTEST ON CACHE BOOL "Build Google Test integration" FORCE)
set(RC_ENABLE_GMOCK ON CACHE BOOL "Build Google Mock integration" FORCE)
mark_as_advanced(RC_ENABLE_GTEST RC_ENABLE_GMOCK)
add_subdirectory(rapidcheck EXCLUDE_FROM_ALL SYSTEM)

macro(build_gtest)
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
endmacro()
mg_build_if_needed(build_gtest "gtest")
mg_create_linkable_if_no_cmake(gtest STATIC "${MG_TOOLCHAIN_ROOT}/lib/libgtest.a" "${MG_TOOLCHAIN_ROOT}/include")
mg_create_linkable_if_no_cmake(gtest_main STATIC "${MG_TOOLCHAIN_ROOT}/lib/libgtest_main.a" "${MG_TOOLCHAIN_ROOT}/include")
mg_create_linkable_if_no_cmake(gmock STATIC "${MG_TOOLCHAIN_ROOT}/lib/libgmock.a" "${MG_TOOLCHAIN_ROOT}/include")
mg_create_linkable_if_no_cmake(gmock_main STATIC "${MG_TOOLCHAIN_ROOT}/lib/libgmock_main.a" "${MG_TOOLCHAIN_ROOT}/include")

# Setup cppitertools
import_header_library(cppitertools ${CMAKE_CURRENT_SOURCE_DIR})

# Setup json
FetchContent_Declare(
    nlohmann_json
    URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
    URL_HASH SHA256=d6c65aca6b1ed68e7a182f4757257b107ae403032760ed6ef121c9d55e81757d
    PATCH_COMMAND patch -p1 < "${CMAKE_CURRENT_SOURCE_DIR}/nlohmann_json3.11.3.patch"
    # FIND_PACKAGE_ARGS # maybe needed for FETCHCONTENT_TRY_FIND_PACKAGE_MODE OPT_IN
)
FetchContent_MakeAvailable(nlohmann_json)

import_external_library(rocksdb STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/rocksdb/lib/librocksdb.a
  ${CMAKE_CURRENT_SOURCE_DIR}/rocksdb/include
  CMAKE_ARGS -DUSE_RTTI=ON
    -DWITH_TESTS=OFF
    -DGFLAGS_NOTHREADS=OFF
    -DCMAKE_INSTALL_LIBDIR=lib
    -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=true
    -DPORTABLE=ON
  BUILD_COMMAND $(MAKE) rocksdb)

macro(build_bcrypt)
  import_external_library(libbcrypt STATIC
    ${CMAKE_CURRENT_SOURCE_DIR}/libbcrypt/bcrypt.a
    ${CMAKE_CURRENT_SOURCE_DIR}
    CONFIGURE_COMMAND sed s/-Wcast-align// -i ${CMAKE_CURRENT_SOURCE_DIR}/libbcrypt/crypt_blowfish/Makefile
    BUILD_COMMAND make -C ${CMAKE_CURRENT_SOURCE_DIR}/libbcrypt
    CC=${CMAKE_C_COMPILER}
    CXX=${CMAKE_CXX_COMPILER}
    INSTALL_COMMAND true)
endmacro()
mg_build_if_needed(build_bcrypt "bcrypt")
mg_create_linkable_if_no_cmake(libbcrypt STATIC
  "${MG_TOOLCHAIN_ROOT}/lib/bcrypt.a"
  "${MG_TOOLCHAIN_ROOT}/include"
)

import_external_library(mgclient STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/mgclient/lib/libmgclient.a
  ${CMAKE_CURRENT_SOURCE_DIR}/mgclient/include
  CMAKE_ARGS -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
  -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
  -DBUILD_TESTING=OFF
  -DBUILD_CPP_BINDINGS=ON)
# NOTE: the following is possible because both imported targets are called the
# same. This setting should be moved under mgclient lib itself.
find_package(OpenSSL REQUIRED)
target_link_libraries(mgclient INTERFACE ${OPENSSL_LIBRARIES})

macro(build_mgconsole)
    add_external_project(mgconsole
      SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/mgconsole
      CMAKE_ARGS
      -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_BINARY_DIR}
      BUILD_COMMAND $(MAKE) mgconsole)
    add_custom_target(mgconsole DEPENDS mgconsole-proj)
endmacro()
mg_build_if_needed(build_mgconsole "mgconsole")

# Setup spdlog
set(SPDLOG_FMT_EXTERNAL ON)
add_subdirectory(spdlog SYSTEM)

macro(build_kafka)
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
      -DWITH_SASL=ON)
    target_link_libraries(librdkafka INTERFACE ${OPENSSL_LIBRARIES} ZLIB::ZLIB sasl2)
    import_library(librdkafka++ STATIC
      ${CMAKE_CURRENT_SOURCE_DIR}/librdkafka/lib/librdkafka++.a
      ${CMAKE_CURRENT_SOURCE_DIR}/librdkafka/include
    )
    target_link_libraries(librdkafka++ INTERFACE librdkafka)
endmacro()
mg_build_if_needed(build_kafka "librdkafka")
mg_create_linkable_if_no_cmake(librdkafka STATIC "${MG_TOOLCHAIN_ROOT}/lib/librdkafka.a" "${MG_TOOLCHAIN_ROOT}/include")
target_link_libraries(librdkafka INTERFACE ${OPENSSL_LIBRARIES} ZLIB::ZLIB sasl2)
mg_create_linkable_if_no_cmake(librdkafka++ STATIC "${MG_TOOLCHAIN_ROOT}/lib/librdkafka++.a" "${MG_TOOLCHAIN_ROOT}/include")
target_link_libraries(librdkafka++ INTERFACE librdkafka)

macro(build_protobuf)
    set(PROTOBUF_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/protobuf/lib)
    import_external_library(protobuf STATIC
      ${PROTOBUF_ROOT}/lib/libprotobuf.a
      ${PROTOBUF_ROOT}/include
      BUILD_IN_SOURCE 1
      CONFIGURE_COMMAND true)
endmacro()
mg_build_if_needed(build_protobuf "protobuf")

macro(build_pulsar)
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
endmacro()
mg_build_if_needed(build_pulsar "pulsar")
mg_create_linkable_if_no_cmake(pulsar STATIC "${MG_TOOLCHAIN_ROOT}/lib/libpulsarwithdeps.a" "${MG_TOOLCHAIN_ROOT}/include")

macro(build_librdtsc)
  if(${MG_ARCH} STREQUAL "ARM64")
    set(MG_LIBRDTSC_CMAKE_ARGS -DLIBRDTSC_ARCH_x86=OFF -DLIBRDTSC_ARCH_ARM64=ON)
  endif()
  import_external_library(librdtsc STATIC
    ${CMAKE_CURRENT_SOURCE_DIR}/librdtsc/lib/librdtsc.a
    ${CMAKE_CURRENT_SOURCE_DIR}/librdtsc/include
    CMAKE_ARGS ${MG_LIBRDTSC_CMAKE_ARGS}
    BUILD_COMMAND $(MAKE) rdtsc)
endmacro()
mg_build_if_needed(build_librdtsc "librdtsc")
mg_create_linkable_if_no_cmake(librdtsc STATIC "${MG_TOOLCHAIN_ROOT}/lib/librdtsc.a" "${MG_TOOLCHAIN_ROOT}/include")

# setup ctre
import_header_library(ctre ${CMAKE_CURRENT_SOURCE_DIR})

# setup absl (cmake sub_directory tolerant)
set(ABSL_PROPAGATE_CXX_STD ON)
add_subdirectory(absl EXCLUDE_FROM_ALL SYSTEM)

# macro(link_jmalloc)
set_path_external_library(jemalloc STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/jemalloc/lib/libjemalloc.a
  ${CMAKE_CURRENT_SOURCE_DIR}/jemalloc/include/)
# endmacro()
# mg_build_if_needed(link_jmalloc "jemalloc")
# if(DEFINED ENV{MG_TOOLCHAIN_VERSION})
#   if($ENV{MG_TOOLCHAIN_VERSION} GREATER_EQUAL 6)
#     set_path_external_library(jemalloc STATIC "${MG_TOOLCHAIN_ROOT}/lib/libjemalloc.a" "${MG_TOOLCHAIN_ROOT}/include")
#   endif()
# endif()

import_header_library(rangev3 ${CMAKE_CURRENT_SOURCE_DIR}/rangev3/include)

macro(build_nuraft)
  import_external_library(nuraft STATIC
    ${CMAKE_CURRENT_SOURCE_DIR}/nuraft/lib/libnuraft.a
    ${CMAKE_CURRENT_SOURCE_DIR}/nuraft/include/)
endmacro()
mg_build_if_needed(build_nuraft "nuraft")
mg_create_linkable_if_no_cmake(nuraft STATIC "${MG_TOOLCHAIN_ROOT}/lib/libnuraft.a" "${MG_TOOLCHAIN_ROOT}/include")
find_package(OpenSSL REQUIRED)
target_link_libraries(nuraft INTERFACE ${OPENSSL_LIBRARIES})

macro(build_mgcxx)
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
endmacro()
mg_build_if_needed(build_mgcxx "mgcxx")
mg_create_linkable_if_no_cmake(tantivy_text_search STATIC "${MG_TOOLCHAIN_ROOT}/lib/libtantivy_text_search.a" "${MG_TOOLCHAIN_ROOT}/include")
mg_create_linkable_if_no_cmake(mgcxx_text_search STATIC "${MG_TOOLCHAIN_ROOT}/lib/libmgcxx_text_search.a" "${MG_TOOLCHAIN_ROOT}/include")
target_link_libraries(tantivy_text_search INTERFACE ${CMAKE_DL_LIBS})
target_link_libraries(mgcxx_text_search INTERFACE ${CMAKE_DL_LIBS})

# setup strong_type (cmake sub_directory tolerant)
add_subdirectory(strong_type EXCLUDE_FROM_ALL SYSTEM)

# setup usearch
import_header_library(usearch ${CMAKE_CURRENT_SOURCE_DIR}/usearch/include)
set_property(TARGET usearch PROPERTY INTERFACE_INCLUDE_DIRECTORIES
  ${CMAKE_CURRENT_SOURCE_DIR}/usearch/include
  ${CMAKE_CURRENT_SOURCE_DIR}/usearch/fp16/include
  ${CMAKE_CURRENT_SOURCE_DIR}/usearch/stringzilla/include)

# suppress usearch-specific #warnings like:
# "It's recommended to use SimSIMD and fp16lib for half-precision numerics"
target_compile_options(usearch INTERFACE -Wno-cpp)

# setup croncpp
add_subdirectory(croncpp EXCLUDE_FROM_ALL SYSTEM)
