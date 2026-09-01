#!/bin/bash
# Loads the feature tests and defines the order in which they run. Shared by
# test_single.bash (Docker) and test_k8s.bash (Kubernetes) so that both
# deployments are checked with exactly the same tests.
SUITE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SUITE_DIR/utils.bash"

for test_file_path in "$SUITE_DIR/features/"*; do
  if [ "$(basename $test_file_path)" == "README.md" ]; then
    continue
  fi
  source $test_file_path
  echo "Loaded $test_file_path..."
done

# Runs every feature test against whatever is listening on
# $MEMGRAPH_DEFAULT_HOST:$MEMGRAPH_BOLT_PORT.
#   $1 - image type: memgraph|mage (only test_query_modules cares).
#   $2 - deployment: docker|k8s. Almost every test only needs Bolt and runs
#        under both; the exceptions are gated below.
#   $3 - mode: normal|fips. The FIPS image is built without the embedded Python
#        interpreter, so features that need in-container Python are skipped.
#        Gated per-test rather than kept as a second list so there is one place
#        to read, and so a new feature is covered by both modes by default.
run_feature_tests() {
  __image_type="${1:-mage}"
  __deployment="${2:-docker}"
  __mode="${3:-normal}"
  # NOTE: test_auth_roles runs in run_auth_feature_tests, once the users exist.
  test_basic_auth
  test_query
  # The expected procedure/function count comes from scanning the repo, which
  # includes the Python modules; a Python-less image legitimately has fewer.
  if [ "$__mode" != "fips" ]; then
    test_query_modules $__image_type
  else
    echo "SKIP FEATURE: query modules (no embedded Python in the FIPS image)"
  fi
  test_session_trace
  test_show_schema_info
  test_spatial
  test_storage
  test_streams
  test_ttl
  test_type_constraints
  test_vector_search
  test_dynamic_algos
  test_functions
  test_label_operations
  test_regex
  test_edge_type_operations
  test_composite_indices
  test_monitoring
  test_multi_tenancy
  test_nested_indices
  test_or_expression_for_labels
  test_shortest_paths
  test_text_search
  test_durability
  # NOTE: LOAD CSV/JSONL/PARQUET read /data, which is bind-mounted into the
  # Docker container but is not present inside the k8s pod. LOAD CSV via SSL
  # fetches over the network -> it runs everywhere.
  if [ "$__deployment" == "docker" ]; then
    test_load_csv
  else
    echo "SKIP FEATURE: LOAD CSV (/data is not mounted into the pod)"
  fi
  test_load_csv_ssl
  if [ "$__deployment" == "docker" ]; then
    test_load_jsonl
    test_load_parquet
  else
    echo "SKIP FEATURE: LOAD JSONL, LOAD PARQUET (/data is not mounted into the pod)"
  fi
  test_parallel_runtime
  test_mgconsole "1.7"
  if [ "$__mode" == "fips" ]; then
    run_fips_compliance_tests
  else
    test_fips_info_disabled
  fi
}

run_fips_compliance_tests() {
  test_fips_show_info
  test_fips_provider_active
  test_fips_drbg_from_provider
  test_fips_non_approved_algorithms_unavailable
  test_fips_no_bundled_openssl
}

# NOTE: If the tested instance is NOT restarted (each test having their own
# instance), all the auth tests have to come after all tests that assume there
# are no users. -> create_test_users + run_auth_feature_tests run last.
create_test_users() {
  # NOTE: TO USER is required -> "admin" is also a built-in role (created with
  # the first user when the enterprise license is valid), so a bare "TO admin"
  # is ambiguous.
  echo "CREATE USER admin IDENTIFIED BY 'admin1234'; GRANT ALL PRIVILEGES TO USER admin;" | $MGCONSOLE_DEFAULT
  echo "CREATE USER tester IDENTIFIED BY 'tester1234'; GRANT CREATE TO tester; GRANT CREATE ON NODES CONTAINING LABELS * TO tester; GRANT DELETE ON NODES CONTAINING LABELS * TO tester; GRANT READ, SET PROPERTY {*} ON NODES CONTAINING LABELS * TO tester; GRANT READ, SET PROPERTY {*} ON EDGES OF TYPE * TO tester; GRANT DATABASE memgraph TO tester;" | $MGCONSOLE_ADMIN
  echo "SHOW USERS;" | $MGCONSOLE_ADMIN
  echo "SHOW ACTIVE USERS;" | $MGCONSOLE_ADMIN
  echo "NOTE: admin and tester users are created for testing purposes."
}

#   $1 - mode: normal|fips (see run_feature_tests).
run_auth_feature_tests() {
  __mode="${1:-normal}"
  test_show_database_settings
  test_auth_roles
  test_impersonate_user
  test_user_profiles
  test_user_role_functions
  if [ "$__mode" == "fips" ]; then
    test_fips_password_hashing
  fi
}
