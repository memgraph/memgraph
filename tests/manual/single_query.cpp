// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "communication/result_stream_faker.hpp"
#include "license/license.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "utils/on_scope_exit.hpp"

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // parse the first cmd line argument as the query
  if (argc < 2) {
    std::cout << "Usage: ./single_query 'RETURN \"query here\"'" << std::endl;
    exit(1);
  }

  auto data_directory = std::filesystem::temp_directory_path() / "single_query_test";
  memgraph::utils::OnScopeExit([&data_directory] { std::filesystem::remove_all(data_directory); });
  memgraph::storage::Config db_config{.durability = {.storage_directory = data_directory},
                                      .disk = {.main_storage_directory = data_directory / "disk"}};

  memgraph::license::global_license_checker.EnableTesting();
  memgraph::replication::ReplicationState repl_state(memgraph::storage::ReplicationStateRootPath(db_config));
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk(db_config, repl_state);
  auto db_acc_opt = db_gk.access();
  MG_ASSERT(db_acc_opt, "Failed to access db");
  auto &db_acc = *db_acc_opt;
  memgraph::system::System system_state;
  memgraph::query::InterpreterContext interpreter_context(memgraph::query::InterpreterConfig{}, nullptr, &repl_state,
                                                          system_state
#ifdef MG_ENTERPRISE
                                                          ,
                                                          std::nullopt
#endif
  );
  memgraph::query::Interpreter interpreter{&interpreter_context, db_acc};

  ResultStreamFaker stream(db_acc->storage());
  memgraph::query::AllowEverythingAuthChecker auth_checker;
  interpreter.SetUser(auth_checker.GenQueryUser(std::nullopt, std::nullopt));
  auto [header, _1, qid, _2] = interpreter.Prepare(argv[1], {}, {});
  stream.Header(header);
  auto summary = interpreter.PullAll(&stream);
  stream.Summary(summary);
  std::cout << stream;

  return 0;
}
