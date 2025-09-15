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

#include "transaction_dependencies.hpp"
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <iostream>

namespace memgraph::storage {

static auto tx_deps_logger = []() {
  auto sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
  auto logger = std::make_shared<spdlog::logger>("tx_deps_independent", sink);
  logger->set_level(spdlog::level::debug);
  logger->set_pattern("[TXDEPS] [%T.%f] %v");
  return logger;
}();

static bool IsWaitingFor(std::unordered_map<uint64_t, uint64_t> const &deps, uint64_t from, uint64_t to,
                         std::unordered_set<uint64_t> &visited) {
  if (visited.count(from)) {
    return false;
  }
  visited.insert(from);

  auto const it = deps.find(from);
  if (it == deps.cend()) {
    return false;
  } else if (it->second == to) {
    return true;
  } else {
    return IsWaitingFor(deps, it->second, to, visited);
  }
}

static bool IsWaitingFor(std::unordered_map<uint64_t, uint64_t> const &deps, uint64_t from, uint64_t to) {
  std::unordered_set<uint64_t> visited;
  return IsWaitingFor(deps, from, to, visited);
}

void TransactionDependencies::RegisterTransaction(uint64_t transaction_id) {
  std::unique_lock l{mutex_};
  active_transactions_.insert(transaction_id);
  tx_deps_logger->info("T{} REGISTERED", transaction_id);
}

void TransactionDependencies::UnregisterTransaction(uint64_t transaction_id) {
  std::unique_lock l{mutex_};
  active_transactions_.erase(transaction_id);
  tx_deps_logger->info("T{} UNREGISTERED", transaction_id);
  cv_.notify_all();
}

WaitResult TransactionDependencies::WaitFor(uint64_t dependent_transaction_id, uint64_t prerequisite_transaction_id) {
  std::unique_lock l{mutex_};

  if (active_transactions_.count(prerequisite_transaction_id) == 0) {
    return WaitResult::SUCCESS;
  }

  dependencies_[dependent_transaction_id] = prerequisite_transaction_id;

  std::string deps_str;
  for (const auto &[from, to] : dependencies_) {
    deps_str += fmt::format("T{}->{} ", from, to);
  }
  tx_deps_logger->info("T{} waiting for T{}. Dependencies: {}", dependent_transaction_id, prerequisite_transaction_id,
                       deps_str);

  if (IsWaitingFor(dependencies_, prerequisite_transaction_id, dependent_transaction_id)) {
    tx_deps_logger->warn("CIRCULAR DEPENDENCY detected: T{}<->T{}", dependent_transaction_id,
                         prerequisite_transaction_id);
    dependencies_.erase(dependent_transaction_id);
    return WaitResult::CIRCULAR_DEPENDENCY;
  }

  tx_deps_logger->info("T{} STARTING WAIT for T{}", dependent_transaction_id, prerequisite_transaction_id);
  cv_.wait(l, [&]() { return active_transactions_.count(prerequisite_transaction_id) == 0; });
  tx_deps_logger->info("T{} FINISHED WAIT for T{}", dependent_transaction_id, prerequisite_transaction_id);

  dependencies_.erase(dependent_transaction_id);
  return WaitResult::SUCCESS;
}

}  // namespace memgraph::storage
