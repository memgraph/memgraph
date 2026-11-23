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

#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "communication/result_stream_faker.hpp"
#include "disk_test_utils.hpp"
#include "prefetch_properties_flag_guard.hpp"
#include "query/auth_checker.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "query/stream/streams.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"

/// Runs whole queries against a database of the given storage type, which is what it takes to test
/// behaviour that no single operator owns.
///
/// Abstract because the storage directories are named after the suite: two binaries built on this
/// run at the same time, and sharing a directory would make each one's results depend on the other.
template <typename StorageType>
class QueryExecutionFixture : public testing::Test {
 protected:
  virtual std::string SuiteName() const = 0;

  std::optional<memgraph::dbms::DatabaseAccess> db_acc_;
  std::optional<memgraph::query::InterpreterContext> interpreter_context_;
  std::optional<memgraph::query::AllowEverythingAuthChecker> auth_checker_;
  std::optional<memgraph::query::Interpreter> interpreter_;

  std::filesystem::path data_directory;

  std::optional<memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock>>
      repl_state;
  std::optional<memgraph::utils::Gatekeeper<memgraph::dbms::Database>> db_gk;
  std::optional<memgraph::system::System> system_state;

  void SetUp() override {
    data_directory = std::filesystem::temp_directory_path() / ("MG_tests_unit_" + SuiteName());

    auto config = [&]() {
      memgraph::storage::Config config{};
      config.durability.storage_directory = data_directory;
      config.disk.main_storage_directory = config.durability.storage_directory / "disk";
      if constexpr (std::is_same_v<StorageType, memgraph::storage::DiskStorage>) {
        config.disk = disk_test_utils::GenerateOnDiskConfig(SuiteName()).disk;
        config.force_on_disk = true;
      }
      return config;
    }();  // iile

    repl_state.emplace(memgraph::storage::ReplicationStateRootPath(config));
    db_gk.emplace(config);
    auto db_acc_opt = db_gk->access();
    MG_ASSERT(db_acc_opt, "Failed to access db");
    auto &db_acc = *db_acc_opt;
    MG_ASSERT(db_acc->GetStorageMode() == (std::is_same_v<StorageType, memgraph::storage::DiskStorage>
                                               ? memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL
                                               : memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL),
              "Wrong storage mode!");
    db_acc_ = std::move(db_acc);
    system_state.emplace();
    interpreter_context_.emplace(memgraph::query::InterpreterConfig{},
                                 nullptr,
                                 nullptr,
                                 nullptr,
                                 &repl_state.value(),
                                 *system_state,
                                 nullptr
#ifdef MG_ENTERPRISE
                                 ,
                                 nullptr,
                                 nullptr
#endif
    );
    auth_checker_.emplace();
    interpreter_.emplace(&*interpreter_context_, *db_acc_);
    interpreter_->SetUser(auth_checker_->GenQueryUser(std::nullopt, {}));
  }

  void TearDown() override {
    interpreter_ = std::nullopt;
    auth_checker_.reset();
    interpreter_context_ = std::nullopt;
    system_state.reset();
    db_acc_.reset();
    db_gk.reset();
    repl_state.reset();
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(SuiteName());
    }
    std::filesystem::remove_all(data_directory);
  }

  /**
   * Execute the given query and commit the transaction.
   *
   * Return the query results.
   */
  auto Execute(const std::string &query) {
    ResultStreamFaker stream(this->db_acc_->get()->storage());

    auto [header, _1, qid, _2] = interpreter_->Prepare(query, memgraph::query::no_params_fn, {});
    stream.Header(header);
    auto summary = interpreter_->PullAll(&stream);
    stream.Summary(summary);

    return stream.GetResults();
  }

  /// What a query answered, what it left behind, and what it refused: everything about it a reader
  /// can see, so that two plans for it can be held to answering the same.
  struct Outcome {
    std::string rows;
    std::string graph;
    std::string refusal;
  };

  template <class TRows>
  static std::string Spell(TRows const &rows) {
    auto out = std::ostringstream{};
    for (const auto &row : rows) {
      for (const auto &value : row) out << value << '|';
      out << ';';
    }
    return out.str();
  }

  /// Three nodes, two of them pointing at the third, which is what lets one record be reached twice
  /// in a query and so be read after that query has written it.
  void PlantGraph() {
    this->Execute("MATCH (n) DETACH DELETE n");
    this->Execute(
        "CREATE (a1 {id: 1, p: 3, q: 7}), (a2 {id: 2, p: 3, q: 7}), (b {id: 3, p: 3, q: 7}) "
        "CREATE (a1)-[:E]->(b) CREATE (a2)-[:E]->(b)");
  }

  /// Which of the two ways of reading a record's properties a query is to be run with, so that a
  /// call site says which it meant rather than passing a bare true or false.
  enum class Arm : std::uint8_t { kLookupAtATime, kOnePass };

  Outcome RunWith(Arm arm, const std::string &query) {
    PrefetchPropertiesFlagGuard guard{arm == Arm::kOnePass};
    PlantGraph();
    // A plan is held under the query's text, so without this the arm planned second would run the
    // plan the first one left behind and the two would agree about nothing.
    db_acc_->get()->plan_cache()->WithLock([](auto &cache) { cache.reset(); });

    auto outcome = Outcome{};
    try {
      outcome.rows = Spell(this->Execute(query));
    } catch (const std::exception &refused) {
      // The slots a plan takes are named for where they sit, so a refusal naming one would differ
      // between the arms for saying so rather than for what it refused.
      outcome.refusal = std::regex_replace(std::string{refused.what()}, std::regex{"anon[0-9]+"}, "slot");
    }
    outcome.graph = Spell(this->Execute("MATCH (n) RETURN n.id AS id, n.p AS p, n.q AS q ORDER BY id"));
    return outcome;
  }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
