# Rewrite a binary's DT_NEEDED for the Python stable ABI.
#
# The linker writes the SONAME of the libpython it found at link time (e.g.
# `libpython3.12.so.1.0`) into the executable's DT_NEEDED. Since Memgraph
# compiles with `Py_LIMITED_API` and only references abi3-stable symbols, the
# resulting binary will resolve against any libpython3 with version >= the
# floor we promised. To make that work at runtime we need DT_NEEDED to read
# `libpython3.so` instead of the versioned SONAME, so the dynamic linker
# picks up whatever libpython is present on the deployment host (typically
# via a `libpython3.so` symlink that admin installs).
#
# This script does the rewrite via `patchelf`. It is invoked from a POST_BUILD
# step on the `memgraph` target so test runs against the built binary already
# see the abi3 dependency.
#
# Usage:
#   cmake -DBINARY=<path> -DPATCHELF=<patchelf-bin> -P RewriteDtNeededAbi3.cmake

if(NOT BINARY)
    message(FATAL_ERROR "BINARY is required")
endif()
if(NOT PATCHELF)
    message(FATAL_ERROR "PATCHELF is required")
endif()
if(NOT EXISTS "${BINARY}")
    message(FATAL_ERROR "Binary not found: ${BINARY}")
endif()

execute_process(
    COMMAND "${PATCHELF}" --print-needed "${BINARY}"
    OUTPUT_VARIABLE needed_libs
    RESULT_VARIABLE rc
    OUTPUT_STRIP_TRAILING_WHITESPACE)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR "patchelf --print-needed failed (exit ${rc})")
endif()

# Match e.g. libpython3.12.so.1.0, libpython3.10.so.1.0.
string(REGEX MATCH "libpython3\\.[0-9]+m?\\.so\\.[0-9.]+" versioned "${needed_libs}")

if(NOT versioned)
    message(STATUS "${BINARY}: DT_NEEDED already unversioned for Python; nothing to rewrite")
    return()
endif()

execute_process(
    COMMAND "${PATCHELF}" --replace-needed "${versioned}" libpython3.so "${BINARY}"
    RESULT_VARIABLE rc)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR "patchelf --replace-needed failed (exit ${rc})")
endif()

message(STATUS "${BINARY}: DT_NEEDED rewritten ${versioned} -> libpython3.so")
