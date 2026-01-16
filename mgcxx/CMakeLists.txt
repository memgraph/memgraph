cmake_minimum_required(VERSION 3.15)
project(mgcxx VERSION 0.0.3)
set (CMAKE_CXX_STANDARD 20)
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
include(ExternalProject)
include(FetchContent)
option(ENABLE_TESTS "Enable tests" ON)

# NOTE: Be careful with moving this outside of the if block (it should not be
# required) because if outside, the test support libraries will be copied to
# the install directory when this project is used as a dependency.
if (${ENABLE_TESTS})
  file(DOWNLOAD https://raw.githubusercontent.com/memgraph/cmake/main/modules/external-fmt.cmake
       ${CMAKE_BINARY_DIR}/external-fmt.cmake)
  include(${CMAKE_BINARY_DIR}/external-fmt.cmake)
  file(DOWNLOAD https://raw.githubusercontent.com/memgraph/cmake/main/modules/fetch-nlohmann.cmake
       ${CMAKE_BINARY_DIR}/fetch-nlohmann.cmake)
  include(${CMAKE_BINARY_DIR}/fetch-nlohmann.cmake)
  file(DOWNLOAD https://raw.githubusercontent.com/memgraph/cmake/main/modules/fetch-gbenchmark.cmake
       ${CMAKE_BINARY_DIR}/fetch-gbenchmark.cmake)
  include(${CMAKE_BINARY_DIR}/fetch-gbenchmark.cmake)
  file(DOWNLOAD https://raw.githubusercontent.com/memgraph/cmake/main/modules/fetch-gtest.cmake
       ${CMAKE_BINARY_DIR}/fetch-gtest.cmake)
  include(${CMAKE_BINARY_DIR}/fetch-gtest.cmake)
  enable_testing()
endif()

add_subdirectory(text_search)
