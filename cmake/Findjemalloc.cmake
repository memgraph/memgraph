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



# Search for jemalloc library and include directory
# Check MG_TOOLCHAIN_ROOT if available, otherwise use CMAKE_PREFIX_PATH
message(STATUS "Findjemalloc: MG_TOOLCHAIN_ROOT=${MG_TOOLCHAIN_ROOT}")
message(STATUS "Findjemalloc: CMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}")

if(DEFINED MG_TOOLCHAIN_ROOT AND MG_TOOLCHAIN_ROOT)
  set(JEMALLOC_SEARCH_PATHS
    ${MG_TOOLCHAIN_ROOT}/lib
    ${MG_TOOLCHAIN_ROOT}/lib64
  )
  set(JEMALLOC_INCLUDE_PATHS
    ${MG_TOOLCHAIN_ROOT}/include
  )
else()
  # Use CMAKE_PREFIX_PATH which should include toolchain path
  # Also check /opt/toolchain-v7 as default macOS toolchain location
  set(JEMALLOC_SEARCH_PATHS
    /opt/toolchain-v7/lib
    /opt/toolchain-v7/lib64
  )
  set(JEMALLOC_INCLUDE_PATHS
    /opt/toolchain-v7/include
  )
  foreach(path ${CMAKE_PREFIX_PATH})
    list(APPEND JEMALLOC_SEARCH_PATHS ${path}/lib ${path}/lib64)
    list(APPEND JEMALLOC_INCLUDE_PATHS ${path}/include)
  endforeach()
endif()

message(STATUS "Findjemalloc: Searching in ${JEMALLOC_SEARCH_PATHS}")
message(STATUS "Findjemalloc: Include paths: ${JEMALLOC_INCLUDE_PATHS}")

find_library(JEMALLOC_LIBRARY
  NAMES jemalloc libjemalloc
  PATHS ${JEMALLOC_SEARCH_PATHS}
  NO_DEFAULT_PATH
  NO_CMAKE_FIND_ROOT_PATH
)

find_path(JEMALLOC_INCLUDE_DIR
  NAMES jemalloc/jemalloc.h
  PATHS ${JEMALLOC_INCLUDE_PATHS}
  NO_DEFAULT_PATH
  NO_CMAKE_FIND_ROOT_PATH
)

message(STATUS "Findjemalloc: After search - JEMALLOC_LIBRARY=${JEMALLOC_LIBRARY}")
message(STATUS "Findjemalloc: After search - JEMALLOC_INCLUDE_DIR=${JEMALLOC_INCLUDE_DIR}")

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
