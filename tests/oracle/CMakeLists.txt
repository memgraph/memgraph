set(oracle_prefix memgraph__oracle__)

add_custom_target(memgraph__oracle)

find_package(Threads REQUIRED)
find_program(CARGO_EXECUTABLE cargo HINTS "$ENV{HOME}/.cargo/bin")
if(NOT CARGO_EXECUTABLE)
  message(FATAL_ERROR "MG_RUST_ORACLES needs cargo (searched PATH and ~/.cargo/bin)")
endif()

# add_rust_oracle(<name>)
# Builds tests/oracle/<name>/ as a Rust static library exposing a C ABI, and
# the differential test <name>/differential.cpp that links it. The test
# target is memgraph__oracle__<name>; link the C++ code under test to it.
#
# The crate compiles no C or C++ itself, so cargo needs neither the toolchain
# sysroot nor the CMake compiler flags.
function(add_rust_oracle name)
  set(crate_dir ${CMAKE_CURRENT_SOURCE_DIR}/${name})
  set(cargo_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/cargo)
  set(rust_lib ${cargo_dir}/release/lib${name}_oracle.a)
  file(GLOB_RECURSE rust_sources CONFIGURE_DEPENDS ${crate_dir}/src/*.rs)

  add_custom_command(
    OUTPUT ${rust_lib}
    COMMAND ${CMAKE_COMMAND} -E env CARGO_TARGET_DIR=${cargo_dir}
            ${CARGO_EXECUTABLE} build --release --manifest-path ${crate_dir}/Cargo.toml
    DEPENDS ${rust_sources} ${crate_dir}/Cargo.toml
    COMMENT "Building Rust oracle ${name}"
    USES_TERMINAL)
  add_custom_target(${name}_oracle_cargo DEPENDS ${rust_lib})

  add_library(${name}_oracle STATIC IMPORTED GLOBAL)
  set_target_properties(${name}_oracle PROPERTIES
    IMPORTED_LOCATION ${rust_lib}
    INTERFACE_INCLUDE_DIRECTORIES ${crate_dir}
    INTERFACE_LINK_LIBRARIES "Threads::Threads;${CMAKE_DL_LIBS};m")
  add_dependencies(${name}_oracle ${name}_oracle_cargo)

  set(test_target ${oracle_prefix}${name})
  add_executable(${test_target} ${crate_dir}/differential.cpp)
  set_target_properties(${test_target} PROPERTIES OUTPUT_NAME ${name}_differential)
  target_include_directories(${test_target} PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(${test_target} ${name}_oracle GTest::gtest GTest::gtest_main rapidcheck rapidcheck_gtest)
  add_test(NAME ${test_target} COMMAND $<TARGET_FILE:${test_target}>)
  set_tests_properties(${test_target} PROPERTIES LABELS "oracle")
  add_dependencies(memgraph__oracle ${test_target})
endfunction()

add_rust_oracle(egraph)
target_link_libraries(memgraph__oracle__egraph mg::planner)
