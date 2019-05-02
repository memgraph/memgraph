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
