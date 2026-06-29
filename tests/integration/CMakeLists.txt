# Integration test subdirectories. Kept as a single list so the abi3 rewrite
# loop below stays in sync with what's built.
set(integration_test_subdirs
    audit
    auth
    env_variable_check
    fine_grained_access
    flag_check
    init_file
    ldap
    license_info
    run_time_settings
    ssl
    storage_mode
    telemetry
    transactions)

foreach(subdir ${integration_test_subdirs})
    add_subdirectory(${subdir})
endforeach()

# Integration test executables link libpython transitively (via mg-query), so
# the linker bakes a versioned libpython SONAME into their DT_NEEDED. Apply the
# same patchelf rewrite that memgraph and the unit tests get (see
# cmake/functions.cmake), so they use the abi3 loader path and keep working when
# the runtime Python is swapped (e.g. the build/runtime version matrix). The
# helper is a no-op for targets without a libpython dependency, so it is safe to
# apply to every executable here.
foreach(subdir ${integration_test_subdirs})
    get_property(subdir_targets
        DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/${subdir}"
        PROPERTY BUILDSYSTEM_TARGETS)
    foreach(target ${subdir_targets})
        get_target_property(target_type ${target} TYPE)
        if(target_type STREQUAL "EXECUTABLE")
            mg_apply_python_abi3_rewrite(${target})
        endif()
    endforeach()
endforeach()
