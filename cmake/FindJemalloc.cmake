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

#find_path(Jemalloc_INCLUDE_DIR NAMES jemalloc/jemalloc.h PATH_SUFFIXES include)

#find_library(Jemalloc_LIBRARY NAMES libjemalloc.a PATH_SUFFIXES lib)

if(Jemalloc_INCLUDE_DIR)
  message(STATUS "find jemalloc: ${Jemalloc_INCLUDE_DIR}")
else()
  message(WARNING "find jemalloc not found!")
endif()

# Check and print where libjemalloc.a was found
if(Jemalloc_LIBRARY)
  message(STATUS "find jemalloc: libjemalloc.a found at: ${Jemalloc_LIBRARY}")
else()
  message(WARNING "libjemalloc.a not found!")
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Jemalloc
  FOUND_VAR Jemalloc_FOUND
  REQUIRED_VARS
    Jemalloc_LIBRARY
    Jemalloc_INCLUDE_DIR
)



message(STATUS "JEMALLOC FOUND ${Jemalloc_FOUND}")



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
  message(STATUS "JEMALLOC NOT TARGET")

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

# set(JEMALLOC_ROOT "/opt/toolchain-vExperiment/")

# message(STATUS "Using JEMALLOC_ROOT: ${JEMALLOC_ROOT}")

# find_path(Jemalloc_INCLUDE_DIR
#           NAMES jemalloc/jemalloc.h
#           HINTS "${JEMALLOC_ROOT}/include"
#           NO_DEFAULT_PATH)

# find_library(Jemalloc_LIBRARY
#              NAMES libjemalloc.a
#              HINTS "${JEMALLOC_ROOT}/lib"
#              NO_DEFAULT_PATH)

# if(Jemalloc_INCLUDE_DIR)
#   message(STATUS "jemalloc.h found at: ${Jemalloc_INCLUDE_DIR}")
# else()
#   message(WARNING "jemalloc.h not found!")
# endif()

# # Check and print where libjemalloc.a was found
# if(Jemalloc_LIBRARY)
#   message(STATUS "libjemalloc.a found at: ${Jemalloc_LIBRARY}")
# else()
#   message(WARNING "libjemalloc.a not found!")
# endif()
