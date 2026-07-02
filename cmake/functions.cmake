# prints all included directories
function(list_includes)
    get_property(dirs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                      PROPERTY INCLUDE_DIRECTORIES)
    foreach(dir ${dirs})
          message(STATUS "dir='${dir}'")
    endforeach()
endfunction(list_includes)

# get file names from list of file paths
function(get_file_names file_paths file_names)
    set(file_names "")
    foreach(file_path ${file_paths})
        get_filename_component (file_name ${file_path} NAME_WE)
        list(APPEND file_names ${file_name})
    endforeach()
    set(file_names "${file_names}" PARENT_SCOPE)
endfunction()

MACRO(SUBDIRLIST result curdir)
    FILE(GLOB children RELATIVE ${curdir} ${curdir}/*)
    SET(dirlist "")
    FOREACH(child ${children})
        IF(IS_DIRECTORY ${curdir}/${child})
            LIST(APPEND dirlist ${child})
        ENDIF()
    ENDFOREACH()
    SET(${result} ${dirlist})
ENDMACRO()

function(disallow_in_source_build)
    get_filename_component(src_dir ${CMAKE_SOURCE_DIR} REALPATH)
    get_filename_component(bin_dir ${CMAKE_BINARY_DIR} REALPATH)
    message(STATUS "SOURCE_DIR" ${src_dir})
    message(STATUS "BINARY_DIR" ${bin_dir})
    # Do we maybe want to limit out-of-source builds to be only inside a
    # directory which contains 'build' in name?
    if("${src_dir}" STREQUAL "${bin_dir}")
        # Unfortunately, we cannot remove CMakeCache.txt and CMakeFiles here
        # because they are written after cmake is done.
        message(FATAL_ERROR "In source build is not supported! "
                "Remove CMakeCache.txt and CMakeFiles and then create a separate "
                "directory, e.g. 'build' and run cmake there.")
    endif()
endfunction()

# Takes a string of ';' separated VALUES and stores a new string in RESULT,
# where ';' is replaced with given SEP.
function(join values sep result)
    # Match non escaped ';' and replace it with separator. This doesn't handle
    # the case when backslash is escaped, e.g: "a\\\\;b" will produce "a;b".
    string(REGEX REPLACE "([^\\]|^);" "\\1${sep}" tmp "${values}")
    # Fix-up escapes by matching backslashes and removing them.
    string(REGEX REPLACE "[\\](.)" "\\1" tmp "${tmp}")
    set(${result} "${tmp}" PARENT_SCOPE)
endfunction()

# Defines a custom target driven by a stamp file, so Ninja only re-runs the
# commands when DEPENDS change. Saves repeating the OUTPUT/touch/add_custom_target
# trio for every file-copy step.
#
# Usage:
#   mg_add_stamped_custom_target(name
#       STAMP_DIR <dir>          # where the .<name>.stamp file lives
#       [ALL]                    # build by default
#       [COMMENT <text>]
#       COMMANDS COMMAND ... [COMMAND ...]...
#       DEPENDS  <files/targets>...
#   )
function(mg_add_stamped_custom_target name)
    cmake_parse_arguments(ARG "ALL" "STAMP_DIR;COMMENT" "COMMANDS;DEPENDS" ${ARGN})
    if(NOT ARG_STAMP_DIR)
        message(FATAL_ERROR "mg_add_stamped_custom_target(${name}): STAMP_DIR is required")
    endif()
    if(NOT ARG_COMMANDS)
        message(FATAL_ERROR "mg_add_stamped_custom_target(${name}): COMMANDS is required")
    endif()
    set(stamp "${ARG_STAMP_DIR}/.${name}.stamp")
    add_custom_command(
        OUTPUT ${stamp}
        ${ARG_COMMANDS}
        COMMAND ${CMAKE_COMMAND} -E touch ${stamp}
        DEPENDS ${ARG_DEPENDS}
        COMMENT "${ARG_COMMENT}"
        VERBATIM
    )
    if(ARG_ALL)
        add_custom_target(${name} ALL DEPENDS ${stamp})
    else()
        add_custom_target(${name} DEPENDS ${stamp})
    endif()
endfunction()

# Returns a list of compile flags ready for gcc or clang.
function(get_target_cxx_flags target result)
    # First set the CMAKE_CXX_FLAGS variables, then append directory and target
    # options in that order. Definitions come last, directory then target.
    string(TOUPPER ${CMAKE_BUILD_TYPE} build_type)
    set(flags "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${build_type}}")
    get_directory_property(dir_opts DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                           COMPILE_OPTIONS)
    if(dir_opts)
        join("${dir_opts}" " " dir_opts)
        string(APPEND flags " " ${dir_opts})
    endif()
    get_target_property(opts ${target} COMPILE_OPTIONS)
    if(opts)
        join("${opts}" " " opts)
        string(APPEND flags " " ${opts})
    endif()
    get_directory_property(dir_defs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                           COMPILE_DEFINITIONS)
    if(dir_defs)
        join("${dir_defs}" " -D" dir_defs)
        string(APPEND flags " -D" ${dir_defs})
    endif()
    get_target_property(defs ${target} COMPILE_DEFINITIONS)
    if(defs)
        join("${defs}" " -D" defs)
        string(APPEND flags " -D" ${defs})
    endif()
    set(${result} ${flags} PARENT_SCOPE)
endfunction()

# Attach a POST_BUILD step to `target_name` that rewrites the binary's
# DT_NEEDED libpython entry from the versioned SONAME the linker recorded
# (e.g. `libpython3.12.so.1.0`) to the unversioned abi3 SONAME
# `libpython3.so`. The compile-time `Py_LIMITED_API` guarantee means the
# binary's Python symbol set is a subset of the stable ABI, so any libpython3
# >= the floor satisfies it at runtime — provided a `libpython3.so` file
# exists on the dynamic linker's search path.
#
# Gated on three conditions (configure-time):
#   1. The `MG_PYTHON_REWRITE_DT_NEEDED` cache variable is ON (default).
#   2. `patchelf` is installed.
#   3. `libpython3.so` (the abi3 SONAME) can be located via find_library; if
#      it isn't on the host then the patched binary couldn't load, which
#      would break downstream POST_BUILD steps that execute the binary (e.g.
#      `config/generate.py`).
#
# Safe to call multiple times — `find_program` and `find_library` results
# are cached, and the underlying CMake helper script is a no-op when the
# binary already has an unversioned libpython dependency.
function(mg_apply_python_abi3_rewrite target_name)
    if(NOT DEFINED CACHE{MG_PYTHON_REWRITE_DT_NEEDED})
        option(MG_PYTHON_REWRITE_DT_NEEDED
               "Rewrite memgraph binaries' DT_NEEDED libpython entry to the abi3 SONAME"
               ON)
    endif()
    if(NOT MG_PYTHON_REWRITE_DT_NEEDED)
        return()
    endif()

    find_program(PATCHELF_EXECUTABLE patchelf)
    find_library(MG_LIBPYTHON3_SO python3)
    if(NOT PATCHELF_EXECUTABLE OR NOT MG_LIBPYTHON3_SO)
        # Warning is emitted once from src/CMakeLists.txt — don't spam here.
        return()
    endif()

    add_custom_command(TARGET ${target_name} POST_BUILD
        COMMAND ${CMAKE_COMMAND}
                -DBINARY=$<TARGET_FILE:${target_name}>
                -DPATCHELF=${PATCHELF_EXECUTABLE}
                -P ${CMAKE_SOURCE_DIR}/cmake/RewriteDtNeededAbi3.cmake
        COMMENT "Rewriting DT_NEEDED on ${target_name} for Python stable-ABI portability"
        VERBATIM)
endfunction()

# Wrap add_executable() so every executable defined after this point gets the
# abi3 DT_NEEDED rewrite attached automatically, in its own directory (which is
# where `add_custom_command(TARGET ...)` must live).
#
# Safe to apply to all executables: mg_apply_python_abi3_rewrite is a no-op when
# the option is off or patchelf/libpython3.so are missing, and the underlying
# script is a no-op for binaries whose DT_NEEDED has no versioned libpython.
function(add_executable name)
    _add_executable(${name} ${ARGN})
    if("${ARGN}" MATCHES "(^|;)(ALIAS|IMPORTED)(;|$)")
        return()
    endif()
    mg_apply_python_abi3_rewrite(${name})
endfunction()
