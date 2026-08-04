# Find the GNU Readline library.
# This module plugs into CMake's `find_package` so the example usage is:
# `find_package(Readline REQUIRED)`
# Options to `find_package` are as documented in CMake documentation.
# READLINE_LIBRARY will be a path to the library.
# READLINE_INCLUDE_DIR will be a path to the include directory.
# READLINE_FOUND will be TRUE if the library is found.
#
# If the library is found, an imported target `readline` will be provided. This
# can be used for linking via `target_link_libraries`, without the need to
# explicitly include READLINE_INCLUDE_DIR and link with READLINE_LIBRARY. For
# example: `target_link_libraries(my_executable readline)`.
find_library(READLINE_LIBRARY readline)
find_path(READLINE_INCLUDE_DIR readline/readline.h)
mark_as_advanced(READLINE_LIBRARY READLINE_INCLUDE_DIR)

if (READLINE_LIBRARY AND READLINE_INCLUDE_DIR)
  set(READLINE_FOUND TRUE)
  if (NOT READLINE_FIND_QUIETLY AND NOT TARGET readline)
    message(STATUS "Found Readline: ${READLINE_LIBRARY} ${READLINE_INCLUDE_DIR}")
  endif()
  # The imported target must be (re)created on EVERY configure, including ones
  # that hit the find_* cache — targets are not persisted in the cache, and a
  # missing target degrades `target_link_libraries(... readline)` to a bare
  # `-lreadline` the sysroot-pinned linker cannot resolve.
  if (NOT TARGET readline)
    add_library(readline SHARED IMPORTED)
    # CMake suppresses `/usr/include` from generated command lines, and under
    # the toolchain sysroot it is not searched implicitly either — the headers
    # silently go missing. Exposing ALL of host /usr/include (e.g. via
    # -idirafter) is not safe: it changes libstdc++'s glibc feature detection
    # (__has_include(<sys/single_threaded.h>) finds the HOST glibc's header
    # and emits __libc_single_threaded references the sysroot glibc cannot
    # satisfy at link time). Instead expose ONLY the readline headers through
    # a shim directory holding a single `readline` symlink.
    if (READLINE_INCLUDE_DIR STREQUAL "/usr/include")
      set(_readline_shim "${CMAKE_BINARY_DIR}/readline-include-shim")
      file(MAKE_DIRECTORY "${_readline_shim}")
      if (NOT EXISTS "${_readline_shim}/readline")
        file(CREATE_LINK "${READLINE_INCLUDE_DIR}/readline" "${_readline_shim}/readline" SYMBOLIC)
      endif()
      set_property(TARGET readline PROPERTY INTERFACE_INCLUDE_DIRECTORIES "${_readline_shim}")
    else()
      set_property(TARGET readline PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${READLINE_INCLUDE_DIR})
    endif()
    set_property(TARGET readline PROPERTY IMPORTED_LOCATION ${READLINE_LIBRARY})
  endif()
else()
  set(READLINE_FOUND FALSE)
  if (READLINE_FIND_REQUIRED)
    message(FATAL_ERROR "Could not find Readline")
  elseif (NOT READLINE_FIND_QUIETLY)
    message(STATUS "Could not find Readline")
  endif()
endif()
