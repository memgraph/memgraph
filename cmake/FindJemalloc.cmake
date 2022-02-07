# Try to find jemalloc library
#
# Use this module as:
#   find_package(Jemalloc)
#
# or:
#   find_package(Jemalloc REQUIRED)
#
# This will define the following variables:
#
#   Jemalloc_FOUND           True if the system has the jemalloc library.
#   Jemalloc_INCLUDE_DIRS    Include directories needed to use jemalloc.
#   Jemalloc_LIBRARIES       Libraries needed to link to jemalloc.
#
# The following cache variables may also be set:
#
# Jemalloc_INCLUDE_DIR     The directory containing jemalloc/jemalloc.h.
# Jemalloc_LIBRARY         The path to the jemalloc static library.

find_path(Jemalloc_INCLUDE_DIR NAMES jemalloc/jemalloc.h PATH_SUFFIXES include)

find_library(Jemalloc_LIBRARY NAMES libjemalloc.a PATH_SUFFIXES lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Jemalloc
  FOUND_VAR Jemalloc_FOUND
  REQUIRED_VARS
    Jemalloc_LIBRARY
    Jemalloc_INCLUDE_DIR
)

if(Jemalloc_FOUND)
  set(Jemalloc_LIBRARIES ${Jemalloc_LIBRARY})
  set(Jemalloc_INCLUDE_DIRS ${Jemalloc_INCLUDE_DIR})
else()
  if(Jemalloc_FIND_REQUIRED)
    message(FATAL_ERROR "Cannot find jemalloc!")
  else()
    message(WARNING "jemalloc is not found!")
  endif()
endif()

if(Jemalloc_FOUND AND NOT TARGET Jemalloc::Jemalloc)
  add_library(Jemalloc::Jemalloc UNKNOWN IMPORTED)
  set_target_properties(Jemalloc::Jemalloc
    PROPERTIES
      IMPORTED_LOCATION "${Jemalloc_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${Jemalloc_INCLUDE_DIR}"
  )
endif()

mark_as_advanced(
  Jemalloc_INCLUDE_DIR
  Jemalloc_LIBRARY
)