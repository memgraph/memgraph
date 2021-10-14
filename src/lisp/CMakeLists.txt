# Lisp C++ Preprocessing

# Don't forget to repeat this list below in `define_add_lcp`.
set(lcp_src_files
    ${PROJECT_SOURCE_DIR}/src/lisp/lcp.asd
    ${PROJECT_SOURCE_DIR}/src/lisp/compile-lcp
    ${PROJECT_SOURCE_DIR}/src/lisp/package.lisp
    ${PROJECT_SOURCE_DIR}/src/lisp/names.lisp
    ${PROJECT_SOURCE_DIR}/src/lisp/types.lisp
    ${PROJECT_SOURCE_DIR}/src/lisp/clone.lisp
    ${PROJECT_SOURCE_DIR}/src/lisp/code-gen.lisp
    ${PROJECT_SOURCE_DIR}/src/lisp/slk.lisp
    ${PROJECT_SOURCE_DIR}/src/lisp/lcp.lisp
    ${PROJECT_SOURCE_DIR}/src/lisp/debug.lisp
    ${PROJECT_SOURCE_DIR}/src/lisp/test.lisp
    ${PROJECT_SOURCE_DIR}/src/lisp/util.lisp
    ${PROJECT_SOURCE_DIR}/tools/lcp)

# Make `lcp_src_files` a persistent (cache) variable so that
# tests/unit/CMakeLists.txt can see it.
set(lcp_src_files "${lcp_src_files}" CACHE INTERNAL "")

add_custom_target(lcp
  DEPENDS ${lcp_src_files}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/compile-lcp)

# Define `add_lcp` function named `name` for registering a lcp file for generation.
#
# The `define_add_lcp` expects 3 arguments:
#   * name -- name for the function, you usually want `add_lcp`
#   * main_src_files -- variable to be updated with generated cpp files
#   * generated_lcp_files -- variable to be updated with generated hpp and cpp files
#
# The `add_lcp` function expects at least a single argument, path to lcp file.
# Each added file is standalone and we avoid recompiling everything.
#
# By default, each `.lcp` file will produce a `.hpp` and `.cpp` file. To tell
# CMake that no `.cpp` file will be generated, pass a NO_CPP option.
macro(define_add_lcp name main_src_files generated_lcp_files)
  function(${name} lcp_file)
    set(options NO_CPP SLK_SERIALIZE)
    set(multi_value_kwargs DEPENDS)
    # NOTE: ${${}ARGN} syntax escapes evaluating macro's ARGN variable; see:
    # https://stackoverflow.com/questions/50365544/how-to-access-enclosing-functions-arguments-from-within-a-macro
    cmake_parse_arguments(KW "${options}" "${one_value_kwargs}" "${multi_value_kwargs}" ${${}ARGN})
    if (IS_ABSOLUTE ${lcp_file})
      string(REGEX REPLACE "\.lcp$" ".hpp" h_file ${lcp_file})
    else()
      string(REGEX REPLACE "\.lcp$" ".hpp" h_file
             "${CMAKE_CURRENT_SOURCE_DIR}/${lcp_file}")
    endif()
    if (NOT KW_NO_CPP)
      if (IS_ABSOLUTE ${lcp_file})
        set(cpp_file ${lcp_file}.cpp)
      else()
        set(cpp_file ${CMAKE_CURRENT_SOURCE_DIR}/${lcp_file}.cpp)
      endif()
      # Update *global* main_src_files
      set(${main_src_files} ${${main_src_files}} ${cpp_file} PARENT_SCOPE)
    endif()
    if (KW_SLK_SERIALIZE)
      set(slk_serialize "SLK_SERIALIZE")
    endif()
    # Repeat the `lcp_src_files` because this is a macro and the variable is
    # not visible when invoked in another file.
    set(lcp_src_files
        ${PROJECT_SOURCE_DIR}/src/lisp/lcp.asd
        ${PROJECT_SOURCE_DIR}/src/lisp/compile-lcp
        ${PROJECT_SOURCE_DIR}/src/lisp/package.lisp
        ${PROJECT_SOURCE_DIR}/src/lisp/names.lisp
        ${PROJECT_SOURCE_DIR}/src/lisp/types.lisp
        ${PROJECT_SOURCE_DIR}/src/lisp/clone.lisp
        ${PROJECT_SOURCE_DIR}/src/lisp/code-gen.lisp
        ${PROJECT_SOURCE_DIR}/src/lisp/slk.lisp
        ${PROJECT_SOURCE_DIR}/src/lisp/lcp.lisp
        ${PROJECT_SOURCE_DIR}/src/lisp/debug.lisp
        ${PROJECT_SOURCE_DIR}/src/lisp/test.lisp
        ${PROJECT_SOURCE_DIR}/src/lisp/util.lisp
        ${PROJECT_SOURCE_DIR}/tools/lcp)
    add_custom_command(OUTPUT ${h_file} ${cpp_file}
      COMMAND ${PROJECT_SOURCE_DIR}/tools/lcp ${lcp_file} ${slk_serialize}
      VERBATIM
      DEPENDS ${lcp_src_files} lcp ${lcp_file}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
    # Update *global* generated_lcp_files
    set(${generated_lcp_files} ${${generated_lcp_files}} ${h_file} ${cpp_file} PARENT_SCOPE)
  endfunction(${name})
endmacro(define_add_lcp)
