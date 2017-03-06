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
