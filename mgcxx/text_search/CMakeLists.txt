string(TOLOWER ${CMAKE_BUILD_TYPE} cmake_lower_build_type)
option(MGCXX_TEXT_SEARCH_VERBOSE "Make cargo build part verbose" OFF)
set(MGCXX_TEXT_SEARCH_CARGO_FLAGS "")
if (MGCXX_TEXT_SEARCH_VERBOSE)
  set(MGCXX_TEXT_SEARCH_CARGO_FLAGS ${MGCXX_TEXT_SEARCH_CARGO_FLAGS} --verbose)
endif()
if (cmake_lower_build_type STREQUAL "debug")
  set(CARGO_CMD cargo build ${MGCXX_TEXT_SEARCH_CARGO_FLAGS})
  set(MGCXX_TEXT_SEARCH_RUST_TARGET_DIR "debug")
else ()
  set(CARGO_CMD cargo build --release ${MGCXX_TEXT_SEARCH_CARGO_FLAGS})
  set(MGCXX_TEXT_SEARCH_RUST_TARGET_DIR "release")
endif ()

# TODO(gitbuda): Make LTO work on Mac because ldd (consider options and default).
option(MGCXX_TEXT_SEARCH_ENABLE_LTO "Enable cross language linking time optimization" OFF)
if (APPLE AND MGCXX_TEXT_SEARCH_ENABLE_LTO)
  message(STATUS "LTO on Apple not possible -> disabling LTO...")
  set(MGCXX_TEXT_SEARCH_ENABLE_LTO OFF)
endif()
if (MGCXX_TEXT_SEARCH_ENABLE_LTO)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT supported OUTPUT error)
  if(supported)
    message(STATUS "IPO / LTO enabled")
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
    add_link_options(-fuse-ld=lld)
  else()
    message(STATUS "IPO / LTO not supported: <${error}>")
  endif()
  set(MGCXX_TEXT_SEARCH_RUST_FLAGS "-Clinker-plugin-lto" "-Clinker=clang" "-Clink-arg=-fuse-ld=lld")
endif()

set(MGCXX_TEXT_SEARCH_BRIDGE_LIB_RS "${CMAKE_CURRENT_SOURCE_DIR}/src/lib.rs")
set(MGCXX_TEXT_SEARCH_BRIDGE_CC     "${CMAKE_CURRENT_BINARY_DIR}/cxxbridge/tantivy_text_search/src/lib.rs.cc")
set(MGCXX_TEXT_SEARCH_BRIDGE_H      "${CMAKE_CURRENT_BINARY_DIR}/cxxbridge/tantivy_text_search/src/lib.rs.h")
set(MGCXX_TEXT_SEARCH_BRIDGE_CXX    "${CMAKE_CURRENT_BINARY_DIR}/cxxbridge/rust/cxx.h")
set(MGCXX_TEXT_SEARCH_RUST_LIB      "${CMAKE_CURRENT_BINARY_DIR}/${MGCXX_TEXT_SEARCH_RUST_TARGET_DIR}/libtantivy_text_search.a")
set(MGCXX_TEXT_SEARCH_RUST_CPP      "${CMAKE_CURRENT_BINARY_DIR}/mgcxx_text_search.cpp")
set(MGCXX_TEXT_SEARCH_RUST_HPP      "${CMAKE_CURRENT_BINARY_DIR}/mgcxx_text_search.hpp")
set(MGCXX_TEXT_SEARCH_RUST_CXX      "${CMAKE_CURRENT_BINARY_DIR}/cxx.hpp")
add_library(mgcxx_text_search STATIC ${MGCXX_TEXT_SEARCH_RUST_CPP})
add_custom_command(
  OUTPUT ${MGCXX_TEXT_SEARCH_RUST_CPP}
  COMMAND CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR} RUSTFLAGS="${MGCXX_TEXT_SEARCH_RUST_FLAGS}" ${CARGO_CMD}
  COMMAND cp ${MGCXX_TEXT_SEARCH_BRIDGE_CC} ${MGCXX_TEXT_SEARCH_RUST_CPP}
  COMMAND cp ${MGCXX_TEXT_SEARCH_BRIDGE_H} ${MGCXX_TEXT_SEARCH_RUST_HPP}
  COMMAND cp ${MGCXX_TEXT_SEARCH_BRIDGE_CXX} ${MGCXX_TEXT_SEARCH_RUST_CXX}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDS ${MGCXX_TEXT_SEARCH_BRIDGE_LIB_RS})
target_link_libraries(mgcxx_text_search ${MGCXX_TEXT_SEARCH_RUST_LIB})
target_include_directories(mgcxx_text_search INTERFACE "${CMAKE_CURRENT_BINARY_DIR}")
add_custom_command(TARGET mgcxx_text_search POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E copy
  ${CMAKE_CURRENT_SOURCE_DIR}/text_search.hpp
  ${CMAKE_CURRENT_BINARY_DIR}/text_search.hpp)
set_target_properties(mgcxx_text_search PROPERTIES PUBLIC_HEADER
  "${CMAKE_CURRENT_BINARY_DIR}/text_search.hpp;${MGCXX_TEXT_SEARCH_RUST_HPP};${MGCXX_TEXT_SEARCH_RUST_CXX}")
install(TARGETS mgcxx_text_search
  LIBRARY DESTINATION lib
  PUBLIC_HEADER DESTINATION include)
install(FILES ${MGCXX_TEXT_SEARCH_RUST_LIB}
  DESTINATION lib)

if (${ENABLE_TESTS})
  add_test(NAME mgcxx_text_search_test
   COMMAND cargo test
   WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  add_executable(test_stress test_stress.cpp)
  target_link_libraries(test_stress PRIVATE fmtlib_static nlohmann_json::nlohmann_json mgcxx_text_search)
  add_executable(test_bench test_bench.cpp)
  target_link_libraries(test_bench PRIVATE fmtlib_static benchmark::benchmark nlohmann_json::nlohmann_json mgcxx_text_search)
  add_executable(test_unit test_unit.cpp)
  target_link_libraries(test_unit gtest gtest_main fmtlib_static nlohmann_json::nlohmann_json mgcxx_text_search)
  add_test(NAME test_unit COMMAND test_unit)
endif()
