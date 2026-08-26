# Fail the build when a debug-info artifact is missing a section it should have.
#
# Both tools involved here fail by producing something plausible rather than by
# returning an error: llvm-dwp writes a well-formed but empty bundle when it
# cannot find the .dwo files a skeleton unit names, and the linker produces no
# accelerator at all when nothing it was given carries the tables to merge.
# Neither says anything, and both only show up when someone tries to debug.
#
# Run with:
#   cmake -DFILE=<file> -DSECTION=<name> -DWHY=<hint> \
#         -DDWARFDUMP=<llvm-dwarfdump> -P VerifyDebugSection.cmake

if(NOT EXISTS "${FILE}")
    message(FATAL_ERROR "expected debug artifact is missing: ${FILE}")
endif()

execute_process(
    COMMAND "${DWARFDUMP}" --show-section-sizes "${FILE}"
    OUTPUT_VARIABLE _sections
    ERROR_VARIABLE _err
    RESULT_VARIABLE _rc)

if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "could not read ${FILE}: ${_err}")
endif()

string(FIND "${_sections}" "${SECTION}" _found)
if(_found EQUAL -1)
    message(FATAL_ERROR "${FILE} has no ${SECTION} section. ${WHY}")
endif()
