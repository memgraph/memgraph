# MAGE Rust query modules, built with cargo (same pattern as
# mgcxx/text_search). Each crate is a cdylib; the built lib<name>.so is
# copied into the dist with the "lib" prefix stripped, since memgraph loads
# modules by bare name.

string(TOLOWER ${CMAKE_BUILD_TYPE} _mage_lc_build_type)
if(_mage_lc_build_type STREQUAL "debug")
    set(MG_MAGE_CARGO_FLAGS "")
    set(MG_MAGE_CARGO_PROFILE_DIR "debug")
else()
    # Release and RelWithDebInfo both map to cargo's release profile.
    set(MG_MAGE_CARGO_FLAGS "--release")
    set(MG_MAGE_CARGO_PROFILE_DIR "release")
endif()

# mage_add_rust_module(<crate dir> <cdylib name without lib prefix>)
function(mage_add_rust_module crate_dir lib_name)
    set(_src ${CMAKE_CURRENT_SOURCE_DIR}/${crate_dir})
    set(_target_dir ${CMAKE_CURRENT_BINARY_DIR}/${crate_dir})
    set(_built ${_target_dir}/${MG_MAGE_CARGO_PROFILE_DIR}/lib${lib_name}.so)
    set(_dist ${MG_MAGE_DIST_DIR}/${lib_name}.so)

    # All crates depend on the local rsmgp-sys bindings crate.
    file(GLOB_RECURSE _rs_sources CONFIGURE_DEPENDS
        ${_src}/src/*.rs
        ${CMAKE_CURRENT_SOURCE_DIR}/rsmgp-sys/src/*.rs)
    list(APPEND _rs_sources
        ${_src}/Cargo.toml
        ${CMAKE_CURRENT_SOURCE_DIR}/rsmgp-sys/Cargo.toml
        ${CMAKE_CURRENT_SOURCE_DIR}/rsmgp-sys/build.rs
        ${CMAKE_CURRENT_SOURCE_DIR}/rsmgp-sys/mgp/mg_procedure.h)

    add_custom_command(
        OUTPUT ${_dist}
        # Scrub C/C++ flags so the root's clang-specific options don't leak
        # into bindgen/cc invocations (mgcxx precedent).
        #
        # Pin bindgen (rsmgp-sys build.rs) to the build's own clang. Build
        # containers ship a bare system libclang without its builtin headers
        # and no clang executable on PATH, so bindgen's clang-sys otherwise
        # fails with "'stddef.h' file not found":
        #   - CLANG_PATH: the executable bindgen probes for builtin include
        #     paths (the load-bearing one). CMAKE_C_COMPILER is the toolchain
        #     clang (environment/toolchain/v7/toolchain.cmake).
        #   - LIBCLANG_PATH: the matching libclang for parsing,
        #   - LD_LIBRARY_PATH: lets that libclang dlopen its own runtime
        #     (same value the toolchain's `activate` exports).
        COMMAND ${CMAKE_COMMAND} -E env
                --unset=CFLAGS --unset=CXXFLAGS
                CLANG_PATH=${CMAKE_C_COMPILER}
                LIBCLANG_PATH=${MG_TOOLCHAIN_ROOT}/lib
                LD_LIBRARY_PATH=${MG_TOOLCHAIN_ROOT}/lib:${MG_TOOLCHAIN_ROOT}/lib64
                CARGO_TARGET_DIR=${_target_dir}
                CARGO_NET_GIT_FETCH_WITH_CLI=true
                cargo build ${MG_MAGE_CARGO_FLAGS}
        COMMAND ${CMAKE_COMMAND} -E copy ${_built} ${_dist}
        WORKING_DIRECTORY ${_src}
        DEPENDS ${_rs_sources}
        COMMENT "Building Rust query module ${lib_name}.so"
        VERBATIM)
    add_custom_target(mage_rust_${lib_name} ALL DEPENDS ${_dist})
    install(PROGRAMS ${_dist}
            DESTINATION ${MG_MAGE_INSTALL_DIR} COMPONENT mage)
    set_property(GLOBAL APPEND PROPERTY MG_MAGE_RUST_TARGETS mage_rust_${lib_name})
endfunction()

mage_add_rust_module(rsmgp-example rust_example)

get_property(_mage_rust_targets GLOBAL PROPERTY MG_MAGE_RUST_TARGETS)
add_custom_target(mage_rust_modules DEPENDS ${_mage_rust_targets})
