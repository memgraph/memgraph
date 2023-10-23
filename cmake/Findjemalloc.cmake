# Try to find jemalloc library
#
# Use this module as:
#   find_package(jemalloc)
#
# or:
#   find_package(jemalloc REQUIRED)
#
# This will define the following variables:
#
#   JEMALLOC_FOUND           True if the system has the jemalloc library.
#   Jemalloc_INCLUDE_DIRS    Include directories needed to use jemalloc.
#   Jemalloc_LIBRARIES       Libraries needed to link to jemalloc.
#
# The following cache variables may also be set:
#
# Jemalloc_INCLUDE_DIR     The directory containing jemalloc/jemalloc.h.
# Jemalloc_LIBRARY         The path to the jemalloc static library.



include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(jemalloc
  FOUND_VAR JEMALLOC_FOUND
  REQUIRED_VARS
    JEMALLOC_LIBRARY
    JEMALLOC_INCLUDE_DIR
)

if(JEMALLOC_INCLUDE_DIR)
  message(STATUS "Found jemalloc include dir: ${JEMALLOC_INCLUDE_DIR}")
else()
  message(WARNING "jemalloc not found!")
endif()

if(JEMALLOC_LIBRARY)
  message(STATUS "Found jemalloc library: ${JEMALLOC_LIBRARY}")
else()
  message(WARNING "jemalloc library not found!")
endif()

if(JEMALLOC_FOUND)
  set(Jemalloc_LIBRARIES ${JEMALLOC_LIBRARY})
  set(Jemalloc_INCLUDE_DIRS ${JEMALLOC_INCLUDE_DIR})
else()
  if(Jemalloc_FIND_REQUIRED)
    message(FATAL_ERROR "Cannot find jemalloc!")
  else()
    message(WARNING "jemalloc is not found!")
  endif()
endif()

if(JEMALLOC_FOUND AND NOT TARGET Jemalloc::Jemalloc)
  message(STATUS "JEMALLOC NOT TARGET")

  add_library(Jemalloc::Jemalloc UNKNOWN IMPORTED)
  set_target_properties(Jemalloc::Jemalloc
    PROPERTIES
      IMPORTED_LOCATION "${JEMALLOC_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${JEMALLOC_INCLUDE_DIR}"
  )
endif()

mark_as_advanced(
  JEMALLOC_INCLUDE_DIR
  JEMALLOC_LIBRARY
)
