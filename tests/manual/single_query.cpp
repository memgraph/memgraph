// Copyright 2023 Memgraph Ltd.
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

  memgraph::license::global_license_checker.EnableTesting();
  memgraph::query::InterpreterContext interpreter_context{memgraph::storage::Config{},
                                                          memgraph::query::InterpreterConfig{}, data_directory};
  memgraph::query::Interpreter interpreter{&interpreter_context};

  ResultStreamFaker stream(interpreter_context.db.get());
  auto [header, _1, qid, _2] = interpreter.Prepare(argv[1], {}, nullptr);
  stream.Header(header);
  auto summary = interpreter.PullAll(&stream);
  stream.Summary(summary);
  std::cout << stream;

  return 0;
}
