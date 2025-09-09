# Ccache configuration for Memgraph builds
# This generates a wrapper script with optimal ccache settings

find_program(CCACHE_PROGRAM ccache)

if(CCACHE_PROGRAM)
  message(STATUS "Found ccache: ${CCACHE_PROGRAM}")

  # Generate the wrapper script with the correct project root
  set(CCACHE_LAUNCHER_SCRIPT "${CMAKE_BINARY_DIR}/ccache_launcher.sh")

  # Set Clang-specific PCH support
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set(CCACHE_PCH_EXTSUM_EXPORT "export CCACHE_PCH_EXTSUM=true")
  else()
    set(CCACHE_PCH_EXTSUM_EXPORT "# PCH_EXTSUM not needed for ${CMAKE_CXX_COMPILER_ID}")
  endif()

  # Use the source directory (this should work whether PROJECT_SOURCE_DIR or CMAKE_SOURCE_DIR is available)
  if(PROJECT_SOURCE_DIR)
    set(CCACHE_PROJECT_ROOT ${PROJECT_SOURCE_DIR})
  else()
    set(CCACHE_PROJECT_ROOT ${CMAKE_SOURCE_DIR})
  endif()

  # Define sloppiness settings in one place
  set(CCACHE_SLOPPINESS_VALUE "pch_defines,time_macros,include_file_mtime,include_file_ctime,gcno_cwd,locale")

  # Use configure_file to generate the script with proper variable substitution
  configure_file(
    "${CMAKE_CURRENT_LIST_DIR}/ccache_launcher.sh.in"
    "${CCACHE_LAUNCHER_SCRIPT}"
    @ONLY
  )

  # Make the script executable
  file(CHMOD ${CCACHE_LAUNCHER_SCRIPT}
    PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
                GROUP_READ GROUP_EXECUTE
                WORLD_READ WORLD_EXECUTE)

  # Use the wrapper script as the compiler launcher
  set(CMAKE_C_COMPILER_LAUNCHER ${CCACHE_LAUNCHER_SCRIPT} CACHE FILEPATH "C compiler launcher")
  set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE_LAUNCHER_SCRIPT} CACHE FILEPATH "C++ compiler launcher")

  # Add compiler flags for better ccache compatibility
  if(CMAKE_BUILD_TYPE STREQUAL "Debug" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    # Make debug info relocatable for better cache hits
    add_compile_options(
      "$<$<COMPILE_LANGUAGE:CXX>:-fdebug-prefix-map=${PROJECT_SOURCE_DIR}=.>"
      "$<$<COMPILE_LANGUAGE:C>:-fdebug-prefix-map=${PROJECT_SOURCE_DIR}=.>"
    )
  endif()

  # For precompiled headers - ensure they work with ccache
  if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.16")
    # Add -fpch-preprocess for GCC to make PCH work with ccache
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
      add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-fpch-preprocess>")
    endif()

    # Add -fno-pch-timestamp for Clang
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
      add_compile_options("$<$<COMPILE_LANGUAGE:CXX>:-fno-pch-timestamp>")
    endif()
  endif()

  # Print ccache statistics target
  add_custom_target(ccache-stats
    COMMAND ${CCACHE_PROGRAM} -s
    COMMENT "Ccache statistics:"
  )

  # Print ccache zero statistics target
  add_custom_target(ccache-zero
    COMMAND ${CCACHE_PROGRAM} -z
    COMMENT "Zeroing ccache statistics"
  )

  message(STATUS "Ccache configured with wrapper script: ${CCACHE_LAUNCHER_SCRIPT}")
  message(STATUS "  PROJECT_ROOT: ${CCACHE_PROJECT_ROOT}")
  message(STATUS "  SLOPPINESS: ${CCACHE_SLOPPINESS_VALUE}")
  message(STATUS "  Run 'make ccache-stats' to see statistics")
  message(STATUS "  Run 'make ccache-zero' to reset statistics")
else()
  message(STATUS "Ccache not found - builds will not be cached")
endif()
