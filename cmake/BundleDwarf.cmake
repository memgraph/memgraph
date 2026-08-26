# Split-DWARF bundling: collect a target's per-TU .dwo files into a single
# .dwp next to the binary, so the debug info travels as one file rather than
# hundreds.
#
# Usage:
#   include(BundleDwarf)
#   mg_bundle_dwarf(my_target [INSTALL_DESTINATION <dir>] [COMPONENT <component>])
#
# Must be called BEFORE mg_split_debug() on the same target. Both attach
# POST_BUILD commands, which run in the order they are added, and
# mg_split_debug strips the binary -- which removes the skeleton units that
# llvm-dwp reads to find the .dwo files in the first place.
#
# No-op when llvm-dwp is absent or the build type produces no .dwo files, so
# callers do not need to guard.

find_program(MG_LLVM_DWP NAMES llvm-dwp dwp HINTS "${MG_TOOLCHAIN_ROOT}/bin")
find_program(MG_LLVM_DWARFDUMP NAMES llvm-dwarfdump HINTS "${MG_TOOLCHAIN_ROOT}/bin")

if(NOT MG_LLVM_DWP)
    message(STATUS "llvm-dwp not found; split-DWARF bundles will not be produced")
endif()

# Captured here because it is read inside a function, where the current list dir
# would otherwise be the caller's.
set(MG_BUNDLE_DWARF_DIR "${CMAKE_CURRENT_LIST_DIR}")

function(mg_bundle_dwarf target)
    # Only RelWithDebInfo carries -g and the dwo_dir link option. Debug emits
    # .dwo files from the frontend next to each object instead, and Release has
    # no debug info to bundle.
    if(NOT MG_LLVM_DWP OR NOT CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
        return()
    endif()

    cmake_parse_arguments(MGBD "" "INSTALL_DESTINATION;COMPONENT" "" ${ARGN})
    if(NOT MGBD_COMPONENT)
        set(MGBD_COMPONENT debuginfo)
    endif()

    # -e reads the executable's skeleton units to locate the .dwo files, rather
    # than being handed a list that would have to match the linker's naming.
    # No BYPRODUCTS: it is evaluated in a scope where $<TARGET_FILE> does not
    # resolve, and naming the .dwp there fails generation with "No target".
    # mg_split_debug leaves it out for the same reason.
    #
    # Run from the source directory: the skeleton units carry a comp_dir the
    # file-prefix-map has made relative to it, and the bundler resolves the .dwo
    # names against that. Getting this wrong produces an empty bundle and a zero
    # exit status, which is what the verify step below catches.
    set(_verify)
    if(MG_LLVM_DWARFDUMP)
        # The index is what maps a skeleton unit's DWO id to its contribution,
        # so a bundle that found no .dwo files has none at all.
        set(_verify
            COMMAND ${CMAKE_COMMAND}
                -DFILE=$<TARGET_FILE:${target}>.dwp
                -DSECTION=.debug_cu_index
                "-DWHY=The .dwo files named by the skeleton units were not found, usually because dwo_dir and the directory the bundler runs in disagree."
                -DDWARFDUMP=${MG_LLVM_DWARFDUMP}
                -P ${MG_BUNDLE_DWARF_DIR}/VerifyDebugSection.cmake)
    endif()

    add_custom_command(TARGET ${target} POST_BUILD
        COMMAND ${MG_LLVM_DWP} -e $<TARGET_FILE:${target}> -o $<TARGET_FILE:${target}>.dwp
        ${_verify}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Bundling split DWARF for ${target}")

    if(MGBD_INSTALL_DESTINATION)
        # gdb finds a .dwp by the binary's own path, so it installs alongside
        # the binary rather than under a build-id path like the sidecar does.
        install(FILES $<TARGET_FILE:${target}>.dwp
            DESTINATION ${MGBD_INSTALL_DESTINATION}
            COMPONENT ${MGBD_COMPONENT}
            OPTIONAL)
    endif()
endfunction()
