// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gflags/gflags.h>

#include "dbms/dbms_handler.hpp"
#include "glue/auth_checker.hpp"
#include "glue/auth_global.hpp"
#include "glue/auth_handler.hpp"
#include "requests/requests.hpp"
#include "storage/v2/config.hpp"
#include "telemetry/telemetry.hpp"
#include "utils/system_info.hpp"
#include "utils/uuid.hpp"

DEFINE_string(endpoint, "http://127.0.0.1:9000/", "Endpoint that should be used for the test.");
DEFINE_int64(interval, 1, "Interval used for reporting telemetry in seconds.");
DEFINE_int64(duration, 10, "Duration of the test in seconds.");
DEFINE_string(storage_directory, "", "Path to the storage directory where to save telemetry data.");
DEFINE_string(root_directory, "", "Path to the database durability root dir.");

#ifdef MG_ENTERPRISE
using memgraph::coordination::CoordinatorInstanceInitConfig;
using memgraph::coordination::CoordinatorState;
#endif

int main(int argc, char **argv) {
  gflags::SetVersionString("telemetry");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Memgraph backend
  std::filesystem::path data_directory{std::filesystem::temp_directory_path() / "MG_telemetry_integration_test"};
  memgraph::auth::SynchedAuth auth_{
      data_directory / "auth",
      memgraph::auth::Auth::Config{std::string{memgraph::glue::kDefaultUserRoleRegex}, "", true}};
  memgraph::glue::AuthQueryHandler auth_handler(&auth_);
  memgraph::glue::AuthChecker auth_checker(&auth_);

  memgraph::storage::Config db_config;
  memgraph::storage::UpdatePaths(db_config, data_directory);
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state(
      ReplicationStateRootPath(db_config));

  memgraph::system::System system_state;
  memgraph::dbms::DbmsHandler dbms_handler(db_config, repl_state
#ifdef MG_ENTERPRISE
                                           ,
                                           auth_, false
#endif
  );
  memgraph::query::InterpreterContext interpreter_context_({}, &dbms_handler, repl_state, system_state
#ifdef MG_ENTERPRISE
                                                           ,
                                                           std::nullopt, nullptr
#endif
                                                           ,
                                                           &auth_handler, &auth_checker);

#ifdef MG_ENTERPRISE
  std::optional<CoordinatorState> coord_state;
  coord_state.emplace(CoordinatorInstanceInitConfig{.coordinator_id = 1,
                                                    .coordinator_port = 20000,
                                                    .bolt_port = 7689,
                                                    .management_port = 10000,
                                                    .durability_dir = "coord_state_dir",
                                                    .coordinator_hostname = "localhost",
                                                    .nuraft_log_file = "test.log",
                                                    .instance_down_timeout_sec = std::chrono::seconds(5),
                                                    .instance_health_check_frequency_sec = std::chrono::seconds(1)});
#endif

  memgraph::requests::Init();
  memgraph::telemetry::Telemetry telemetry(FLAGS_endpoint, FLAGS_storage_directory, memgraph::utils::GenerateUUID(),
                                           memgraph::utils::GetMachineId(), false, FLAGS_root_directory,
                                           std::chrono::seconds(FLAGS_interval), 1);

  // User defined collector
  uint64_t counter = 0;
  telemetry.AddCollector("test", [&counter]() -> nlohmann::json {
    ++counter;
    return {{"vertices", counter}, {"edges", counter}};
  });

  // Memgraph specific collectors
  telemetry.AddStorageCollector(dbms_handler, auth_);
#ifdef MG_ENTERPRISE
  telemetry.AddDatabaseCollector(dbms_handler);
  telemetry.AddCoordinatorCollector(coord_state);
#else
  telemetry.AddDatabaseCollector();
#endif
  telemetry.AddClientCollector();
  telemetry.AddEventsCollector();
  telemetry.AddQueryModuleCollector();
  telemetry.AddExceptionCollector();
  telemetry.AddReplicationCollector(repl_state);
  telemetry.Start();

  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_duration));

  return 0;
}
