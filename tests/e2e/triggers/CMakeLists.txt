find_package(gflags REQUIRED)
find_package(GTest REQUIRED)

add_library(memgraph__e2e__triggers_common STATIC common.hpp common.cpp)
target_link_libraries(memgraph__e2e__triggers_common PUBLIC gflags mgclient::mgclient mg-utils)

add_executable(memgraph__e2e__triggers__on_create on_create_triggers.cpp)
target_link_libraries(memgraph__e2e__triggers__on_create memgraph__e2e__triggers_common)

add_executable(memgraph__e2e__triggers__on_update on_update_triggers.cpp)
target_link_libraries(memgraph__e2e__triggers__on_update memgraph__e2e__triggers_common)

add_executable(memgraph__e2e__triggers__on_delete on_delete_triggers.cpp)
target_link_libraries(memgraph__e2e__triggers__on_delete memgraph__e2e__triggers_common)

add_executable(memgraph__e2e__triggers__privileges privilege_check.cpp)
target_link_libraries(memgraph__e2e__triggers__privileges memgraph__e2e__triggers_common GTest::gtest GTest::gtest_main)

add_subdirectory(procedures)

add_dependencies(memgraph__e2e__triggers__on_create memgraph__e2e__triggers__write.py)
add_dependencies(memgraph__e2e__triggers__on_update memgraph__e2e__triggers__write.py)
add_dependencies(memgraph__e2e__triggers__on_delete memgraph__e2e__triggers__write.py)

function(copy_triggers_e2e_python_files FILE_NAME)
    copy_e2e_python_files(triggers ${FILE_NAME})
endfunction()

copy_triggers_e2e_python_files(common.py)
copy_triggers_e2e_python_files(triggers_properties_false.py)
copy_triggers_e2e_python_files(show_triggers.py)

copy_e2e_files(triggers workloads.yaml)

# Rewrite each e2e helper binary's versioned libpython DT_NEEDED to the abi3
# SONAME (libpython3.so), like memgraph and the unit/integration tests, so they
# keep loading when the runtime Python is swapped. No-op for binaries without a
# libpython dependency.
mg_apply_python_abi3_rewrite_dir_executables()
