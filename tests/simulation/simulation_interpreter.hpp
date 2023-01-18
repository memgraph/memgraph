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

#include "io/simulator/simulator_handle.hpp"
#include "machine_manager/machine_config.hpp"
#include "machine_manager/machine_manager.hpp"
#include "query/v2/config.hpp"
#include "query/v2/discard_value_stream.hpp"
#include "query/v2/frontend/ast/ast.hpp"
#include "query/v2/interpreter.hpp"
#include "query/v2/request_router.hpp"

#include <string>
#include <vector>

// TODO(gvolfing)
// -How to set up the entire raft cluster with the QE. Also provide abrstraction for that.
// -Pass an argument to the setup to determine, how many times the retry of a query should happen.

namespace memgraph::io::simulator {

class SimulatedInterpreter {
  using ResultStream = query::v2::DiscardValueResultStream;

 public:
  explicit SimulatedInterpreter(std::unique_ptr<query::v2::InterpreterContext> interpreter_context)
      : interpreter_context_(std::move(interpreter_context)) {
    interpreter_ = std::make_unique<memgraph::query::v2::Interpreter>(interpreter_context_.get());
  }

  SimulatedInterpreter(const SimulatedInterpreter &) = delete;
  SimulatedInterpreter &operator=(const SimulatedInterpreter &) = delete;
  SimulatedInterpreter(SimulatedInterpreter &&) = delete;
  SimulatedInterpreter &operator=(SimulatedInterpreter &&) = delete;
  ~SimulatedInterpreter() = default;

  void InstallSimulatorTicker(Simulator &simulator) {
    std::function<bool()> tick_simulator = simulator.GetSimulatorTickClosure();
    auto *request_router = interpreter_->GetRequestRouter();
    request_router->InstallSimulatorTicker(tick_simulator);
  }

  std::vector<ResultStream> RunQueries(const std::vector<std::string> &queries) {
    std::vector<ResultStream> results;
    results.reserve(queries.size());

    for (const auto &query : queries) {
      results.emplace_back(RunQuery(query));
    }
    return results;
  }

 private:
  ResultStream RunQuery(const std::string &query) {
    ResultStream stream;

    std::map<std::string, memgraph::storage::v3::PropertyValue> params;
    const std::string *username = nullptr;

    interpreter_->BeginTransaction();

    auto *rr = interpreter_->GetRequestRouter();
    rr->StartTransaction();

    interpreter_->Prepare(query, params, username);
    interpreter_->PullAll(&stream);
    interpreter_->CommitTransaction();

    return stream;
  }

  std::unique_ptr<query::v2::InterpreterContext> interpreter_context_;
  std::unique_ptr<query::v2::Interpreter> interpreter_;
};

SimulatedInterpreter SetUpInterpreter(Address coordinator_address, Simulator &simulator) {
  auto rr_factory = std::make_unique<memgraph::query::v2::SimulatedRequestRouterFactory>(simulator);

  auto interpreter_context = std::make_unique<memgraph::query::v2::InterpreterContext>(
      nullptr,
      memgraph::query::v2::InterpreterConfig{.query = {.allow_load_csv = true},
                                             .execution_timeout_sec = 600,
                                             .replication_replica_check_frequency = std::chrono::seconds(1),
                                             .default_kafka_bootstrap_servers = "",
                                             .default_pulsar_service_url = "",
                                             .stream_transaction_conflict_retries = 30,
                                             .stream_transaction_retry_interval = std::chrono::milliseconds(500)},
      std::filesystem::path("mg_data"), std::move(rr_factory), coordinator_address);

  return SimulatedInterpreter(std::move(interpreter_context));
}

}  // namespace memgraph::io::simulator
