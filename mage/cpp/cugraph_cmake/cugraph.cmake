# Copyright (c) 2016-2022 Memgraph Ltd. [https://memgraph.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# NOTES:
#
# Install Cuda manually from https://developer.nvidia.com/cuda-downloads
# because cugraph requires Cuda 11+. In fact, don't use system Cuda because
# CMake easily detects that one. export PATH="/usr/local/cuda/bin:$PATH" is
# your friend.
#
# INSTALL SYSTEM PACKAGES: sudo apt install libblas-dev liblapack-dev libboost-all-dev
#
# NCCL is also required (NVIDIA Developer Program registration is required ->
# huge hustle). NCCL could be installed from  https://github.com/NVIDIA/nccl.
#
# Order of the languages matters because cmake pics different compilers.
#
# Compiling cugraph takes ages and it's complex.
# TODO(gitbuda): Allow linking of an already compiled version of cugraph.
#
# CUDA_ARCHITECTURES ->
# https://arnon.dk/matching-sm-architectures-arch-and-gencode-for-various-nvidia-cards/
# Rapids CMake add NATIVE + ALL options as CUDA_ARCHITECTURES which simplifies
# build configuration.

option(MAGE_CUGRAPH_ENABLE "Enable cuGraph build" OFF)
message(STATUS "MAGE cuGraph build enabled: ${MAGE_CUGRAPH_ENABLE}")

if (MAGE_CUGRAPH_ENABLE)
  # Version of cuGraph for local build
  set(MAGE_CUGRAPH_TAG "v22.02.00" CACHE STRING "cuGraph GIT tag to checkout" )
  set(MAGE_CUGRAPH_REPO "https://github.com/rapidsai/cugraph.git" CACHE STRING "cuGraph GIT repo URL")
  # Custom MAGE_CUGRAPH_BUILD_TYPE.
  set(MAGE_CUGRAPH_BUILD_TYPE "Release" CACHE STRING "Passed to cuGraph as CMAKE_BUILD_TYPE")
  # NATIVE | ALL -> possible because cugraph calls rapids_cuda_init_architectures
  set(MAGE_CUDA_ARCHITECTURES "NATIVE" CACHE STRING "Passed to cuGraph as CMAKE_CUDA_ARCHITECTURES")

  # RAPIDS.cmake is here because rapids_cuda_init_architectures is required to
  # properly set both CMAKE_CUDA_ARCHITECTURES and CUDA_ARCHITECTURES target
  # property.
  file(DOWNLOAD "https://raw.githubusercontent.com/rapidsai/rapids-cmake/${MAGE_CUGRAPH_TAG}a/RAPIDS.cmake"
                ${CMAKE_BINARY_DIR}/RAPIDS.cmake)
  include(${CMAKE_BINARY_DIR}/RAPIDS.cmake)

  include(rapids-cuda)
  rapids_cuda_init_architectures("${MEMGRAPH_MAGE_PROJECT_NAME}")
  enable_language(CUDA)

  set(CMAKE_CUDA_STANDARD 17)
  set(CMAKE_CUDA_STANDARD_REQUIRED ON)

  message(STATUS "MAGE cuGraph root: ${MAGE_CUGRAPH_ROOT}")
  # Skip downloading if root is configured
  if (NOT MAGE_CUGRAPH_ROOT)
    set(MAGE_CUGRAPH_ROOT ${PROJECT_BINARY_DIR}/cugraph)
    ExternalProject_Add(cugraph-proj
      PREFIX            "${MAGE_CUGRAPH_ROOT}"
      INSTALL_DIR       "${MAGE_CUGRAPH_ROOT}"
      GIT_REPOSITORY    "${MAGE_CUGRAPH_REPO}"
      GIT_TAG           "${MAGE_CUGRAPH_TAG}"
      SOURCE_SUBDIR     "cpp"
      CMAKE_ARGS        "-DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>"
                        "-DCMAKE_BUILD_TYPE=${MAGE_CUGRAPH_BUILD_TYPE}"
                        "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
                        "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
                        "-DCMAKE_CUDA_ARCHITECTURES='${MAGE_CUDA_ARCHITECTURES}'"
                        "-DBUILD_STATIC_FAISS=ON"
                        "-DBUILD_TESTS=OFF"
                        "-DBUILD_CUGRAPH_MG_TESTS=OFF"
    )
  endif()

  set(MAGE_CUGRAPH_INCLUDE_DIR "${MAGE_CUGRAPH_ROOT}/include")
  set(MAGE_CUGRAPH_LIBRARY_PATH "${MAGE_CUGRAPH_ROOT}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}cugraph.so")
  add_library(mage_cugraph SHARED IMPORTED)
  set_target_properties(mage_cugraph PROPERTIES
    IMPORTED_LOCATION "${MAGE_CUGRAPH_LIBRARY_PATH}"
  )
  include_directories("${MAGE_CUGRAPH_INCLUDE_DIR}")
  add_dependencies(mage_cugraph cugraph-proj)
endif()

macro(add_cugraph_subdirectory subdirectory_name)
  if (MAGE_CUGRAPH_ENABLE)
    add_subdirectory("${subdirectory_name}")
  endif()
endmacro()

macro(target_mage_cugraph target_name)
  if (MAGE_CUGRAPH_ENABLE)
    list(APPEND MAGE_CUDA_FLAGS --expt-extended-lambda)
    if(CMAKE_BUILD_TYPE MATCHES Debug)
      message(STATUS "Building with CUDA debugging flags")
      list(APPEND MAGE_CUDA_FLAGS -g -G -Xcompiler=-rdynamic)
    endif()
    target_compile_options("${target_name}"
      PRIVATE "$<$<COMPILE_LANGUAGE:CXX>:${MAGE_CXX_FLAGS}>"
              "$<$<COMPILE_LANGUAGE:CUDA>:${MAGE_CUDA_FLAGS}>"
    )
    target_link_libraries("${target_name}" PRIVATE mage_cugraph)
  endif()
endmacro()
