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
if (READLINE_LIBRARY AND READLINE_INCLUDE_DIR)
  set(READLINE_FOUND TRUE)
else()
  find_library(READLINE_LIBRARY readline)
  find_path(READLINE_INCLUDE_DIR readline/readline.h)
  if (READLINE_LIBRARY AND READLINE_INCLUDE_DIR)
    set(READLINE_FOUND TRUE)
    if (NOT READLINE_FIND_QUIETLY)
      message(STATUS "Found Readline: ${READLINE_LIBRARY} ${READLINE_INCLUDE_DIR}")
    endif()
  else()
    set(READLINE_FOUND FALSE)
    if (READLINE_FIND_REQUIRED)
      message(FATAL_ERROR "Could not find Readline")
    elseif (NOT READLINE_FIND_QUIETLY)
      message(STATUS "Could not find Readline")
    endif()
  endif()
  mark_as_advanced(READLINE_LIBRARY READLINE_INCLUDE_DIR)
  add_library(readline SHARED IMPORTED)
  set_property(TARGET readline PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${READLINE_INCLUDE_DIR})
  set_property(TARGET readline PROPERTY IMPORTED_LOCATION ${READLINE_LIBRARY})
endif()
