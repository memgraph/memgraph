# The bundled toolchain is a directory of compilers and runtime libraries produced by
# environment/toolchain. Everything the rest of the build needs to know about how it is
# laid out is decided here: whether one is in use, which C++ runtime gets linked and
# shipped, and which directory holds the libraries third-party code expects to find.
#
# Consumers link mg::toolchain rather than naming a path, so a host that builds against
# its own system compiler is described by turning the option off, not by editing the
# places that would otherwise spell the layout out.

option(MG_BUNDLED_TOOLCHAIN "Build against the bundled toolchain and package its C++ runtime" ON)

add_library(mg-toolchain INTERFACE)
add_library(mg::toolchain ALIAS mg-toolchain)

# Set when the bundled toolchain supplies the C++ runtime, in which case it has to be
# installed beside the binary that was linked against it.
set(MG_TOOLCHAIN_RUNTIME_LIBRARY "" CACHE INTERNAL "C++ runtime library to ship, if any")
set(MG_TOOLCHAIN_LINK_DIR "" CACHE INTERNAL "Toolchain library directory to add to the link search path")

if (NOT MG_BUNDLED_TOOLCHAIN)
    message(STATUS "Bundled toolchain: off, building against the host's C++ runtime")
    return()
endif ()

if (NOT MG_TOOLCHAIN_ROOT OR NOT IS_DIRECTORY "${MG_TOOLCHAIN_ROOT}")
    message(FATAL_ERROR
            "MG_TOOLCHAIN_ROOT must be set to a valid toolchain directory, or MG_BUNDLED_TOOLCHAIN "
            "must be OFF to build against the host's compiler. Current value: '${MG_TOOLCHAIN_ROOT}'")
endif ()
message(STATUS "MG_TOOLCHAIN_ROOT: ${MG_TOOLCHAIN_ROOT}")

# The runtime libraries sit in lib64 while lib holds what third-party builds link against;
# both are properties of how the toolchain is installed, so they are named only here.
set(MG_TOOLCHAIN_RUNTIME_DIR "${MG_TOOLCHAIN_ROOT}/lib64")
set(MG_TOOLCHAIN_LINK_DIR "${MG_TOOLCHAIN_ROOT}/lib" CACHE INTERNAL "" FORCE)

set(CUSTOM_LIBSTDCXX_DIR "${MG_TOOLCHAIN_RUNTIME_DIR}" CACHE PATH "Directory containing custom libstdc++.so.6")
set(CUSTOM_LIBSTDCXX "${CUSTOM_LIBSTDCXX_DIR}/libstdc++.so.6")

if (NOT EXISTS "${CUSTOM_LIBSTDCXX}")
    message(FATAL_ERROR "Custom libstdc++.so.6 not found at: ${CUSTOM_LIBSTDCXX}. Please ensure the toolchain is properly built.")
endif ()

message(STATUS "Found custom libstdc++.so.6 at: ${CUSTOM_LIBSTDCXX}")

# A file of the right name that is not the runtime the compiler expects would fail at
# load time rather than at link time, so check what it claims to be.
execute_process(
        COMMAND readelf -d "${CUSTOM_LIBSTDCXX}"
        OUTPUT_VARIABLE ELF_INFO
        RESULT_VARIABLE READ_RESULT
)

if (NOT READ_RESULT EQUAL 0 OR NOT ELF_INFO MATCHES "SONAME.*libstdc\\+\\+\\.so\\.6")
    message(FATAL_ERROR "Invalid libstdc++.so.6: bad or missing SONAME. Please ensure the toolchain is properly built.")
endif ()

message(STATUS "Custom libstdc++.so.6 validation successful")

add_library(libstdc++_custom SHARED IMPORTED GLOBAL)
set_target_properties(libstdc++_custom PROPERTIES
        IMPORTED_LOCATION "${CUSTOM_LIBSTDCXX}"
        INTERFACE_LINK_LIBRARIES ""
)

# Linking the bundled runtime means not also linking the host's, which is why these two
# travel together and why a caller should not have to know about either.
target_link_libraries(mg-toolchain INTERFACE libstdc++_custom)
target_link_options(mg-toolchain INTERFACE -nostdlib++)

set(MG_TOOLCHAIN_RUNTIME_LIBRARY "${CUSTOM_LIBSTDCXX}" CACHE INTERNAL "" FORCE)
