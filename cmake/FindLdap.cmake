# Find the OpenLDAP library.
# This module plugs into CMake's `find_package` so the example usage is:
# `find_package(Ldap REQUIRED)`
# Options to `find_package` are as documented in CMake documentation.
# LDAP_LIBRARY will be a path to the library.
# LDAP_INCLUDE_DIR will be a path to the include directory.
# LDAP_FOUND will be TRUE if the library is found.
#
# If the library is found, an imported target `ldap` will be provided. This
# can be used for linking via `target_link_libraries`, without the need to
# explicitly include LDAP_INCLUDE_DIR and link with LDAP_LIBRARY. For
# example: `target_link_libraries(my_executable ldap)`.
if (LDAP_LIBRARY AND LDAP_INCLUDE_DIR)
  set(LDAP_FOUND TRUE)
else()
  find_library(LDAP_LIBRARY ldap)
  find_path(LDAP_INCLUDE_DIR ldap.h)
  if (LDAP_LIBRARY AND LDAP_INCLUDE_DIR)
    set(LDAP_FOUND TRUE)
    if (NOT LDAP_FIND_QUIETLY)
      message(STATUS "Found LDAP: ${LDAP_LIBRARY} ${LDAP_INCLUDE_DIR}")
    endif()
  else()
    set(LDAP_FOUND FALSE)
    if (LDAP_FIND_REQUIRED)
      message(FATAL_ERROR "Could not find LDAP")
    elseif (NOT LDAP_FIND_QUIETLY)
      message(STATUS "Could not find LDAP")
    endif()
  endif()
  mark_as_advanced(LDAP_LIBRARY LDAP_INCLUDE_DIR)
  add_library(ldap SHARED IMPORTED)
  set_property(TARGET ldap PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${LDAP_INCLUDE_DIR})
  set_property(TARGET ldap PROPERTY IMPORTED_LOCATION ${LDAP_LIBRARY})
endif()
