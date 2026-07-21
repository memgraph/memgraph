# Memgraph MAGE C++ query modules.
#
# This tree is only ever configured as part of the root memgraph build
# (add_subdirectory from mage/CMakeLists.txt, gated by MG_BUILD_MAGE).
# Dependencies (fmt, Boost, GTest) come from the root's Conan set; the
# only extra system requirement is the toolchain's OpenMP runtime.
#
# Keep the project() call: cugraph_cmake/cugraph.cmake relies on the
# project scope (rapids_cuda_init_architectures, ${PROJECT_BINARY_DIR}).
set(MEMGRAPH_MAGE_PROJECT_NAME "memgraph-mage")
project("${MEMGRAPH_MAGE_PROJECT_NAME}" LANGUAGES C CXX)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cugraph_cmake)

# MAGE stays on C11/C++20 (directory-scoped; the root tree is C++23).
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# No C++23 modules in MAGE; skip the clang-scan-deps pass.
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Query modules are dlopen'ed by name: no "lib" prefix.
set(CMAKE_SHARED_LIBRARY_PREFIX "")

# MAGE has never built with LTO; keep it off (also required: nvcc rejects
# the root's IPO flags for the cuGraph .cu targets).
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_Release FALSE)
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RelWithDebInfo FALSE)

# Historic MAGE optimization flag for the numeric-heavy algorithms.
# (The old standalone build also passed GCC's -fvect-cost-model=dynamic,
# which clang doesn't support.)
add_compile_options(
    "$<$<AND:$<COMPILE_LANGUAGE:C,CXX>,$<CONFIG:Release,RelWithDebInfo>>:-funsafe-math-optimizations>")

# Wrap the inherited directory-level COMPILE_OPTIONS (the root's -Wall and
# -Werror=... set) in a language guard so they never reach nvcc when the
# cuGraph CUDA targets are enabled.
get_directory_property(_mage_inherited_opts COMPILE_OPTIONS)
set(_mage_wrapped_opts "")
foreach(_opt IN LISTS _mage_inherited_opts)
    list(APPEND _mage_wrapped_opts "$<$<COMPILE_LANGUAGE:C,CXX>:${_opt}>")
endforeach()
set_directory_properties(PROPERTIES COMPILE_OPTIONS "${_mage_wrapped_opts}")

find_package(Threads REQUIRED)
find_package(OpenMP REQUIRED)

# The old standalone build spliced the OpenMP flags into the global C/C++
# flags, and several sources (grappolo, betweenness_centrality_online) use
# `#pragma omp` / omp.h without linking OpenMP::OpenMP_CXX themselves —
# keep OpenMP enabled directory-wide so those pragmas don't silently
# compile out as serial code.
add_compile_options(
    "$<$<COMPILE_LANGUAGE:C>:${OpenMP_C_FLAGS}>"
    "$<$<COMPILE_LANGUAGE:CXX>:${OpenMP_CXX_FLAGS}>")
add_link_options(${OpenMP_CXX_FLAGS})

# Modules that link OpenMP need the toolchain's libomp.so at load time. In
# the build tree that's an rpath into the toolchain; installed modules get
# INSTALL_RPATH $ORIGIN/.. (set in mage_add_query_module) and the mage
# package bundles the toolchain's libomp.so under lib/memgraph — mirroring
# how memgraph bundles its toolchain libstdc++. A distro libomp can't be
# used instead: its packaging is LLVM-version-tied and its soname
# (libomp.so.5 on deb distros) doesn't match the toolchain's `libomp.so`.
if(OpenMP_omp_LIBRARY)
    get_filename_component(_mage_omp_dir "${OpenMP_omp_LIBRARY}" DIRECTORY)
    list(APPEND CMAKE_BUILD_RPATH "${_mage_omp_dir}")
    file(REAL_PATH "${OpenMP_omp_LIBRARY}" _mage_omp_real)
    install(FILES ${_mage_omp_real}
        DESTINATION lib/memgraph
        RENAME libomp.so
        COMPONENT mage)
endif()

include(ExternalProject)

# cuGraph (optional, -DMG_ENABLE_CUGRAPH=ON)
include(cugraph)

# gtest shim: module CMakeLists link `mage_gtest`; the actual gtest comes
# from the root's Conan test requirements.
add_library(mage_gtest INTERFACE)
target_link_libraries(mage_gtest INTERFACE GTest::gtest)

# mg_utility: memgraph's public query-module headers (mgp.hpp,
# mg_procedure.h) plus the MAGE helper headers (mg_utils.hpp, ...).
add_library(mg_utility INTERFACE)
target_include_directories(mg_utility INTERFACE ${CMAKE_SOURCE_DIR}/include)

# Unit tests are skipped in cuGraph builds (historic behavior: GPU CI
# images don't run them).
set(MG_MAGE_BUILD_TESTS OFF)
if(MG_ENABLE_TESTING AND NOT MG_ENABLE_CUGRAPH)
    set(MG_MAGE_BUILD_TESTS ON)
endif()

<<<<<<< HEAD
# Treat fmt as a system dependency: its headers are then included with
# -isystem for every module that links fmt::fmt, so the compiler and
# clang-tidy do not emit diagnostics from inside fmt's own headers.
get_target_property(FMT_INCLUDE_DIRS fmt INTERFACE_INCLUDE_DIRECTORIES)
set_target_properties(fmt PROPERTIES
  INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${FMT_INCLUDE_DIRS}")

FetchContent_Declare(nlohmann_json
  GIT_REPOSITORY https://github.com/nlohmann/json.git
  GIT_TAG v3.11.3
)
FetchContent_MakeAvailable(nlohmann_json)

# Treat nlohmann_json as a system dependency (same rationale as fmt above).
get_target_property(NLOHMANN_JSON_INCLUDE_DIRS nlohmann_json INTERFACE_INCLUDE_DIRECTORIES)
set_target_properties(nlohmann_json PROPERTIES
  INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${NLOHMANN_JSON_INCLUDE_DIRS}")

function(add_query_module target_name version src)
  add_library(${target_name} SHARED ${src})
  set_target_properties(${target_name} PROPERTIES SOVERSION "${version}")
  target_link_libraries(${target_name} PRIVATE mg_utility)
  mg_split_debug(${target_name})
=======
# NOTE: query_modules/CMakeLists.txt defines a similar add_query_module()
# for memgraph's own modules; this one is mage_-prefixed to avoid the
# global-function collision.
function(mage_add_query_module target_name version src)
    add_library(${target_name} SHARED ${src})
    # INSTALL_RPATH: installed modules live in <prefix>/lib/memgraph/
    # query_modules, so $ORIGIN/.. resolves to lib/memgraph where the mage
    # package bundles libomp.so (and memgraph its libstdc++).
    set_target_properties(${target_name} PROPERTIES
        SOVERSION "${version}"
        LIBRARY_OUTPUT_DIRECTORY "${MG_MAGE_DIST_DIR}"
        INSTALL_RPATH "\$ORIGIN/..")
    target_link_libraries(${target_name} PRIVATE mg_utility)
    install(TARGETS ${target_name}
        LIBRARY DESTINATION ${MG_MAGE_INSTALL_DIR} COMPONENT mage
        NAMELINK_COMPONENT mage)
    mg_split_debug(${target_name}
        INSTALL_DESTINATION ${MG_MAGE_INSTALL_DIR}
        COMPONENT mage_debuginfo)
    set_property(GLOBAL APPEND PROPERTY MG_MAGE_MODULE_TARGETS ${target_name})
>>>>>>> 575fc4364 (refactor MAGE build)
endfunction()

# Available query modules.
add_subdirectory(betweenness_centrality_module)
add_subdirectory(biconnected_components_module)
add_subdirectory(bipartite_matching_module)
add_subdirectory(bridges_module)
add_subdirectory(connectivity_module)
add_subdirectory(cycles_module)
add_subdirectory(community_detection_module)
add_subdirectory(pagerank_module)
add_subdirectory(uuid_module)
add_subdirectory(katz_centrality_module)
add_subdirectory(degree_centrality_module)
add_subdirectory(graph_util_module)
add_subdirectory(node_similarity_module)
add_subdirectory(distance_calculator)
add_subdirectory(do_module)
add_subdirectory(periodic_module)
add_subdirectory(util_module)
add_subdirectory(create_module)
add_subdirectory(map_module)
add_subdirectory(collections_module)
add_subdirectory(nodes_module)
add_subdirectory(meta_module)
add_subdirectory(label_module)
add_subdirectory(text_module)
add_subdirectory(path_module)
add_subdirectory(node_module)
add_subdirectory(neighbors_module)
add_subdirectory(refactor_module)
add_subdirectory(merge_module)
add_subdirectory(csv_utils_module)
add_subdirectory(algo_module)
add_subdirectory(set_property_module)
add_subdirectory(leiden_community_detection_module)
add_subdirectory(math_module)
add_subdirectory(knn_module)
add_subdirectory(search_module)

add_cugraph_subdirectory(cugraph_module)
