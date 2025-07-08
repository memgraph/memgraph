copy_e2e_python_files(sso common.py)
copy_e2e_python_files(sso test_sso.py)
copy_e2e_python_files(sso test_oidc.py)
copy_e2e_python_files(sso test_saml_sso_module.py)
copy_e2e_python_files(sso test_multi_tenant_sso.py)
copy_e2e_python_files(sso dummy_sso_module.py)

copy_e2e_files(sso workloads.yaml)

add_subdirectory(data)

copy_e2e_python_files_from_parent_folder(sso ".." memgraph.py)
copy_e2e_python_files_from_parent_folder(sso ".." interactive_mg_runner.py)
copy_e2e_python_files_from_parent_folder(sso ".." mg_utils.py)
copy_e2e_python_files_from_parent_folder(sso "../../../src/auth/reference_modules" oidc.py)
copy_e2e_python_files_from_parent_folder(sso "../../../src/auth/reference_modules" saml.py)
