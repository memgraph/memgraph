find_package(gflags REQUIRED)

copy_e2e_python_files(durability conftest.py)
copy_e2e_python_files(durability common.py)
copy_e2e_python_files(durability durability_with_property_compression_used.py)
copy_e2e_python_files(durability durability_with_text_index.py)
copy_e2e_python_files(durability durability_with_descriptions.py)
copy_e2e_python_files(durability durability_with_vector_index.py)
copy_e2e_python_files(durability durability_with_vector_edge_index.py)
copy_e2e_python_files(durability durability_with_desc_index.py)
copy_e2e_python_files(durability durability_with_vertex_property_index.py)
copy_e2e_python_files(durability periodic_snapshot.py)
copy_e2e_python_files(durability periodic_snapshot_light_edge.py)
copy_e2e_python_files(durability snapshot_recovery.py)
copy_e2e_python_files(durability snapshot_recovery_light_edge.py)
copy_e2e_python_files(durability multitenancy.py)
copy_e2e_python_files(durability indices_not_persisted_analytical.py)
copy_e2e_python_files(durability parameters_durability.py)
copy_e2e_python_files(durability tenant_profile_durability.py)
copy_e2e_python_files(durability recovery_failure_broken.py)
copy_e2e_python_files(durability recovery_failure_status.py)
copy_e2e_python_files(durability recovery_failure_recover_snapshot.py)
copy_e2e_python_files(durability drop_broken_database.py)
copy_e2e_python_files(durability snapshot_no_space.py)

# LD_PRELOAD shim used by snapshot_no_space.py to simulate a full disk; built next to the copied test.
add_library(memgraph__e2e__durability__no_space_preload SHARED no_space_preload.c)
set_target_properties(memgraph__e2e__durability__no_space_preload PROPERTIES
    PREFIX ""
    OUTPUT_NAME no_space_preload
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(memgraph__e2e__durability__no_space_preload ${CMAKE_DL_LIBS})
add_dependencies(memgraph__e2e memgraph__e2e__durability__no_space_preload)



copy_e2e_python_files_from_parent_folder(durability ".." memgraph.py)
copy_e2e_python_files_from_parent_folder(durability ".." interactive_mg_runner.py)
copy_e2e_python_files_from_parent_folder(durability ".." mg_utils.py)

copy_e2e_files(durability workloads.yaml)
