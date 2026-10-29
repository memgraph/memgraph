# Per-step resource profiling of the build (see tools/build_profile/README.md).
#
# Loaded through CMAKE_PROJECT_INCLUDE by tools/build_profile/profile.sh, so the
# regular build is untouched. It arms itself only while the driver's
# MG_BUILD_PROFILE_LOG environment variable is set: a later configure that still
# carries the cache entry but runs outside the driver is a normal build.
if("$ENV{MG_BUILD_PROFILE_LOG}" STREQUAL "")
    return()
endif()
if(NOT CMAKE_GENERATOR MATCHES "Ninja")
    message(WARNING "Build profiling needs the Ninja generator; not enabled under '${CMAKE_GENERATOR}'")
    return()
endif()
# project() runs more than once (sub-projects); arm the launchers once.
get_property(_mg_bp_armed GLOBAL PROPERTY RULE_LAUNCH_COMPILE)
if(_mg_bp_armed)
    return()
endif()

if(NOT "$ENV{MG_PYTHON}" STREQUAL "")
    set(_mg_bp_python "$ENV{MG_PYTHON}")
else()
    find_program(_mg_bp_python NAMES python3 REQUIRED)
endif()
set(_mg_bp_sample_ms 50)
if(NOT "$ENV{MG_BUILD_PROFILE_SAMPLE_MS}" STREQUAL "")
    set(_mg_bp_sample_ms "$ENV{MG_BUILD_PROFILE_SAMPLE_MS}")
endif()

# -S -I: no site-packages, no env influence; keeps the wrapper's startup ~20 ms.
set(_mg_bp_wrap "${_mg_bp_python} -S -I ${CMAKE_CURRENT_LIST_DIR}/step.py"
                " --log $ENV{MG_BUILD_PROFILE_LOG} --sample-ms ${_mg_bp_sample_ms}")
string(CONCAT _mg_bp_wrap ${_mg_bp_wrap})
set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE
    "${_mg_bp_wrap} --kind compile --target <TARGET_NAME> --language <LANGUAGE> --source <SOURCE> --output <OBJECT> --")
set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK
    "${_mg_bp_wrap} --kind link --target <TARGET_NAME> --language <LANGUAGE> --target-type <TARGET_TYPE> --output <TARGET> --")
set_property(GLOBAL PROPERTY RULE_LAUNCH_CUSTOM
    "${_mg_bp_wrap} --kind custom --target <TARGET_NAME> --output <OUTPUT> --")
message(STATUS "Build profiling: per-step resource log -> $ENV{MG_BUILD_PROFILE_LOG}")
