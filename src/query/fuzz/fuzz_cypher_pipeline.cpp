// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#include "communication/result_stream_faker.hpp"
#include "dbms/database.hpp"
#include "query/auth_checker.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "system/state.hpp"
#include "utils/exceptions.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/synchronized.hpp"

namespace {

struct FuzzEnvironment {
  FuzzEnvironment() {
    data_dir_ = std::filesystem::temp_directory_path() / "mg_fuzz_pipeline";
    std::filesystem::remove_all(data_dir_);

    memgraph::storage::Config config{};
    config.durability.storage_directory = data_dir_;

    db_gk_ = std::make_unique<memgraph::utils::Gatekeeper<memgraph::dbms::Database>>(config);
    auto db_acc_opt = db_gk_->access();
    db_acc_ = std::make_unique<memgraph::dbms::DatabaseAccess>(std::move(*db_acc_opt));

    interpreter_context_ = std::make_unique<memgraph::query::InterpreterContext>(memgraph::query::InterpreterConfig{},
                                                                                 nullptr,
                                                                                 nullptr,
                                                                                 nullptr,
                                                                                 &repl_state_,
                                                                                 system_state_,
                                                                                 nullptr
#ifdef MG_ENTERPRISE
                                                                                 ,
                                                                                 nullptr,
                                                                                 nullptr
#endif
    );

    interpreter_ = std::make_unique<memgraph::query::Interpreter>(interpreter_context_.get(), *db_acc_);
    interpreter_context_->auth_checker = &auth_checker_;
    interpreter_context_->interpreters.WithLock(
        [this](auto &interpreters) { interpreters.insert(interpreter_.get()); });
    interpreter_->SetUser(auth_checker_.GenQueryUser(std::nullopt, {}));
  }

  ~FuzzEnvironment() {
    interpreter_context_->interpreters.WithLock([this](auto &interpreters) { interpreters.erase(interpreter_.get()); });
    interpreter_.reset();
    interpreter_context_.reset();
    db_acc_.reset();
    db_gk_.reset();
    std::filesystem::remove_all(data_dir_);
  }

  void RunQuery(std::string const &query) {
    auto const [header, _1, qid, _2] =
        interpreter_->Prepare(query, [](auto *) { return memgraph::storage::ExternalPropertyValue::map_t{}; }, {});
    auto &db = interpreter_->current_db_.db_acc_;
    ResultStreamFaker stream(db ? db->get()->storage() : nullptr);
    stream.Header(header);
    interpreter_->Pull(&stream, {}, qid);
  }

  void RunSetup(std::string const &path) {
    std::ifstream file(path);
    if (!file.is_open()) return;
    std::string line;
    while (std::getline(file, line)) {
      if (line.empty() || line[0] == '/' || line[0] == '#') continue;
      // Strip trailing semicolon
      if (!line.empty() && line.back() == ';') line.pop_back();
      if (line.empty()) continue;
      try {
        RunQuery(line);
      } catch (std::exception const &e) {
        std::cerr << "Setup query failed: " << e.what() << "\n  Query: " << line << "\n";
      }
    }
  }

  std::filesystem::path data_dir_;
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state_{
      std::nullopt};
  memgraph::system::System system_state_;
  memgraph::query::AllowEverythingAuthChecker auth_checker_;
  std::unique_ptr<memgraph::utils::Gatekeeper<memgraph::dbms::Database>> db_gk_;
  std::unique_ptr<memgraph::dbms::DatabaseAccess> db_acc_;
  std::unique_ptr<memgraph::query::InterpreterContext> interpreter_context_;
  std::unique_ptr<memgraph::query::Interpreter> interpreter_;
};

FuzzEnvironment *GetEnvironment() {
  static auto *env = new FuzzEnvironment();
  return env;
}

bool g_setup_done = false;

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(uint8_t const *data, size_t size) {
  if (size == 0 || size > 4096) return 0;

  auto *env = GetEnvironment();

  if (!g_setup_done) {
    env->RunSetup("seed_corpus_fraud_detection/setup.cypher");
    g_setup_done = true;
  }

  std::string const query(reinterpret_cast<char const *>(data), size);

  try {
    env->RunQuery(query);
  } catch (memgraph::utils::BasicException const &) {
  } catch (std::exception const &) {
  }

  return 0;
}
