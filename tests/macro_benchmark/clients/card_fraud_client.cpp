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

#include <memory>
#include <random>
#include <shared_mutex>
#include <vector>

#include "gflags/gflags.h"

#include "utils/rw_lock.hpp"

#include "long_running_common.hpp"

std::atomic<int64_t> num_pos;
std::atomic<int64_t> num_cards;
std::atomic<int64_t> num_transactions;
std::atomic<int64_t> max_tx_id;

memgraph::utils::RWLock world_lock(memgraph::utils::RWLock::Priority::WRITE);

DEFINE_string(config, "", "test config");

enum class Role { WORKER, ANALYTIC, CLEANUP };

int64_t NumNodesWithLabel(Client &client, std::string label) {
  std::string query = fmt::format("MATCH (u :{}) RETURN count(u)", label);
  auto result = ExecuteNTimesTillSuccess(client, query, {}, MAX_RETRIES);
  return result.first.records[0][0].ValueInt();
}

int64_t MaxIdForLabel(Client &client, std::string label) {
  std::string query = fmt::format("MATCH (u :{}) RETURN max(u.id)", label);
  auto result = ExecuteNTimesTillSuccess(client, query, {}, MAX_RETRIES);
  return result.first.records[0][0].ValueInt();
}

void CreateIndex(Client &client, const std::string &label, const std::string &property) {
  spdlog::info("Creating indexes for :{}({})...", label, property);
  ExecuteNTimesTillSuccess(client, fmt::format("CREATE INDEX ON :{}({});", label, property), {}, MAX_RETRIES);
  try {
    spdlog::info("Trying to sync indexes...");
    ExecuteNTimesTillSuccess(client, "CALL db.awaitIndexes(14400);", {}, MAX_RETRIES);
  } catch (memgraph::utils::BasicException &e) {
    spdlog::warn("Index sync failed: {}", e.what());
  }
}

class CardFraudClient : public TestClient {
 public:
  CardFraudClient(int id, nlohmann::json config, Role role = Role::WORKER)
      : TestClient(), rg_(id), role_(role), config_(config) {}

 private:
  std::mt19937 rg_;
  Role role_;
  nlohmann::json config_;

  void GetFraudulentTransactions() {
    auto result = Execute(
        "MATCH (t:Transaction {fraud_reported: true}) "
        "RETURN t.id as id",
        {}, "GetFraudulentTransactions");
    MG_ASSERT(result, "Read-only query should not fail!");
  }

  /* This query could be rewritten into an equivalent one:
   * 'MATCH (t:Transaction {fraud_reported: true}) RETURN t LIMIT 1'.
   * When written like this it causes a lot of network traffic between
   * distributed workers because they have to return all their data to master
   * instead of just one node, but doesn't overload the client because it has to
   * process just one return value.
   */
  void HeavyRead() {
    auto result = Execute(
        "MATCH (t:Transaction {fraud_reported: true}) "
        "WITH COLLECT(t.id) as ids "
        "RETURN head(ids)",
        {}, "HeavyRead");
    MG_ASSERT(result, "Read-only query should not fail");
  }

  /* If a card was used in a fraudulent transaction, we mark all POS it was used
   * on as possibly fraudulent (having a connected fraud). This query counts
   * connected frauds for each POS and orders them by that number.
   */
  /* TODO(mtomic): this would make more sense if the data was timestamped. */
  auto GetCompromisedPos(int pos_limit) {
    auto result = Execute(
        "MATCH (t:Transaction {fraud_reported: true})-[:Using]->(:Card)"
        "<-[:Using]-(:Transaction)-[:At]->(p:Pos) "
        "WITH p.id as pos, count(t) as connected_frauds "
        "WHERE connected_frauds > 1 "
        "RETURN pos, connected_frauds "
        "ORDER BY connected_frauds DESC LIMIT $pos_limit",
        {{"pos_limit", pos_limit}}, "GetCompromisedPos");
    MG_ASSERT(result, "Read-only query should not fail");
  }

  /* The following two queries approximate the above one. `UpdateFraudScores`
   * computes the number of connected frauds for each POS incrementally, as
   * transactions are added, and `GetCompromisedPosInc` just orders them by that
   * value. Both queries should execute reasonably fast, while the above one can
   * take a lot of time if the dataset is large. The `fraud_score` value
   * computed is not equal to `connected_frauds` computed in the above query
   * because there could be multiple paths from a transaction to a POS. */
  void UpdateFraudScores(int64_t tx_id) {
    auto result = Execute(
        "MATCH (t:Transaction {id: "
        "$tx_id})-[:Using]->(:Card)<-[:Using]-(:Transaction)-[:At]->(p:Pos) "
        "SET p.connected_frauds = p.connected_frauds + 1",
        {{"tx_id", tx_id}}, "UpdateFraudScores");
    if (!result) {
      spdlog::warn("`UpdateFraudScores` failed too many times!");
    }
  }

  void GetCompromisedPosInc(int64_t pos_limit) {
    auto result = Execute(
        "MATCH (p:Pos) "
        "RETURN p, p.connected_frauds "
        "ORDER BY p.connected_frauds DESC "
        "LIMIT $pos_limit",
        {{"pos_limit", pos_limit}}, "GetCompromisedPosInc");
    MG_ASSERT(result, "Read-only query should not fail");
  }

  /* This is used to approximate Memgraph's TEPS (traversed edges per seconds)
   * metric by multiplying the throughput with the size of the dataset. */
  void TepsQuery() {
    auto result = Execute("MATCH (u)--(v) RETURN count(1)", {}, "TepsQuery");
    MG_ASSERT(result, "Read-only query should not fail");
    MG_ASSERT(result->records[0][0].ValueInt() == num_transactions * 4, "Wrong count returned from TEPS query");
  }

  /* Simple point lookup. */
  void GetTransaction(int64_t id) {
    auto result = Execute("MATCH (t:Transaction {id: $id}) RETURN (t)", {{"id", id}}, "GetTransaction");
    MG_ASSERT(result, "Read-only query should not fail");
    MG_ASSERT(result->records[0][0].ValueVertex().properties["id"].ValueInt() == id,
              "Transaction with wrong ID returned from point lookup");
  }

  void CreateTransaction(int64_t pos_id, int64_t card_id, int64_t tx_id, bool is_fraud) {
    auto result = Execute(
        "MATCH (p:Pos {id: $pos_id}), (c:Card {id: $card_id}) "
        "CREATE (c)<-[:Using]-(t:Transaction {id: $tx_id, fraud_reported: "
        "$is_fraud})-[:At]->(p) RETURN t",
        {{"pos_id", pos_id}, {"card_id", card_id}, {"tx_id", tx_id}, {"is_fraud", is_fraud}}, "CreateTransaction");

    if (!result) {
      spdlog::warn("`CreateTransaction` failed too many times!");
      return;
    }

    MG_ASSERT(result->records.size() == 1,
              "Failed to create transaction: (:Card {{id: {}}})<-(:Transaction "
              "{{id: {}}})->(:Pos {{id: {}}})",
              card_id, tx_id, pos_id);

    num_transactions++;
  }

  int64_t UniformInt(int64_t a, int64_t b) {
    std::uniform_int_distribution<int64_t> dist(a, b);
    return dist(rg_);
  }

  double UniformDouble(double a, double b) {
    std::uniform_real_distribution<double> dist(a, b);
    return dist(rg_);
  }

 public:
  void AnalyticStep() {
    std::this_thread::sleep_for(std::chrono::milliseconds(config_["analytic"]["query_interval_ms"]));
    std::shared_lock<memgraph::utils::RWLock> lock(world_lock);
    GetCompromisedPosInc(config_["analytic"]["pos_limit"]);
  }

  void WorkerStep() {
    std::shared_lock<memgraph::utils::RWLock> lock(world_lock);
    bool is_fraud = UniformDouble(0, 1) < config_["fraud_probability"];

    int64_t pos_id = UniformInt(0, num_pos - 1);
    int64_t pos_worker = pos_id / config_["pos_per_worker"].get<int64_t>();

    int64_t card_worker = pos_worker;

    if (config_["num_workers"] > 1 && UniformDouble(0, 1) < config_["hop_probability"]) {
      card_worker = UniformInt(0, config_["num_workers"].get<int64_t>() - 2);
      if (card_worker >= pos_worker) {
        ++card_worker;
      }
    }

    int64_t card_id = card_worker * config_["cards_per_worker"].get<int64_t>() +
                      UniformInt(0, config_["cards_per_worker"].get<int64_t>() - 1);

    int64_t tx_id = ++max_tx_id;
    CreateTransaction(pos_id, card_id, tx_id, is_fraud);
    if (is_fraud) {
      UpdateFraudScores(tx_id);
    }
  }

  int64_t NumTransactions() {
    auto result = Execute("MATCH (t:Transaction) RETURN count(1)", {}, "NumTransactions");
    MG_ASSERT(result, "Read-only query should not fail!");
    return result->records[0][0].ValueInt();
  }

  void CleanupStep() {
    if (num_transactions >= config_["cleanup"]["tx_hi"].get<int64_t>()) {
      spdlog::info("Trying to obtain world lock...");
      std::unique_lock<memgraph::utils::RWLock> lock(world_lock);
      int64_t id_limit = max_tx_id - config_["cleanup"]["tx_lo"].get<int>() + 1;
      spdlog::info(
          "Transaction cleanup started, deleting transactions "
          "with ids less than {}",
          id_limit);
      memgraph::utils::Timer timer;
      auto result = Execute(
          "MATCH (t:Transaction) WHERE t.id < $id_limit "
          "DETACH DELETE t RETURN count(1)",
          {{"id_limit", id_limit}}, "TransactionCleanup");
      int64_t deleted = 0;
      if (result) {
        deleted = result->records[0][0].ValueInt();
      } else {
        spdlog::error("Transaction cleanup failed");
      }
      spdlog::info("Deleted {} transactions in {} seconds", deleted, timer.Elapsed().count());
      int64_t num_transactions_db = NumTransactions();
      MG_ASSERT(num_transactions_db == num_transactions - deleted,
                "Number of transactions after deletion doesn't match: "
                "before = {}, after = {}, reported deleted = {}, actual = "
                "{}",
                num_transactions, num_transactions_db, deleted, num_transactions - num_transactions_db);
      num_transactions = num_transactions_db;
    }

    std::this_thread::sleep_for(std::chrono::seconds(config_["cleanup"]["check_interval_sec"]));
  }

  void Step() override {
    if (FLAGS_scenario == "heavy_read") {
      HeavyRead();
      return;
    }

    if (FLAGS_scenario == "teps") {
      TepsQuery();
      return;
    }

    if (FLAGS_scenario == "point_lookup") {
      GetTransaction(UniformInt(0, num_transactions - 1));
      return;
    }

    if (FLAGS_scenario == "create_tx") {
      CreateTransaction(UniformInt(0, num_pos - 1), UniformInt(0, num_cards - 1), ++max_tx_id, false);
      return;
    }

    /* Card fraud demo using incremental approach to computing fraud scores for
     * POS. New transactions are created at the maximum possible rate. Analytic
     * query is ran periodically. Transaction cleanup is run when the number of
     * transactions exceeds provided limit to prevent memory overflow. All other
     * queries are halted during transaction cleanup (synchronization by shared
     * mutex).
     */
    if (FLAGS_scenario == "card_fraud_inc") {
      switch (role_) {
        case Role::ANALYTIC:
          AnalyticStep();
          break;
        case Role::WORKER:
          WorkerStep();
          break;
        case Role::CLEANUP:
          CleanupStep();
          break;
      }
      return;
    }

    LOG_FATAL("Should not get here: unknown scenario!");
  }
};

nlohmann::json LoadConfig() {
  nlohmann::json config;
  if (FLAGS_config != "") {
    spdlog::info("Loading config from: {}", FLAGS_config);
    std::ifstream is(FLAGS_config);
    is >> config;
  } else {
    spdlog::info("No test config provided");
  }
  return config;
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  memgraph::communication::SSLInit sslInit;

  Endpoint endpoint(FLAGS_address, FLAGS_port);
  ClientContext context(FLAGS_use_ssl);
  Client client(context);
  client.Connect(endpoint, FLAGS_username, FLAGS_password);

  num_pos.store(NumNodesWithLabel(client, "Pos"));
  num_cards.store(NumNodesWithLabel(client, "Card"));
  num_transactions.store(NumNodesWithLabel(client, "Transaction"));
  max_tx_id.store(MaxIdForLabel(client, "Transaction"));

  CreateIndex(client, "Pos", "id");
  CreateIndex(client, "Card", "id");
  CreateIndex(client, "Transaction", "fraud_reported");
  CreateIndex(client, "Transaction", "id");
  spdlog::info("Done building indexes.");

  client.Close();

  auto config = LoadConfig();

  std::vector<std::unique_ptr<TestClient>> clients;
  if (FLAGS_scenario == "card_fraud_inc") {
    MG_ASSERT(FLAGS_num_workers >= 2, "There should be at least 2 client workers (analytic and cleanup)");
    MG_ASSERT(num_pos == config["num_workers"].get<int>() * config["pos_per_worker"].get<int>(),
              "Wrong number of POS per worker");
    MG_ASSERT(num_cards == config["num_workers"].get<int>() * config["cards_per_worker"].get<int>(),
              "Wrong number of cards per worker");
    for (int i = 0; i < FLAGS_num_workers - 2; ++i) {
      clients.emplace_back(std::make_unique<CardFraudClient>(i, config));
    }
    clients.emplace_back(std::make_unique<CardFraudClient>(FLAGS_num_workers - 2, config, Role::ANALYTIC));
    clients.emplace_back(std::make_unique<CardFraudClient>(FLAGS_num_workers - 1, config, Role::CLEANUP));
  } else {
    for (int i = 0; i < FLAGS_num_workers; ++i) {
      clients.emplace_back(std::make_unique<CardFraudClient>(i, config));
    }
  }

  RunMultithreadedTest(clients);

  return 0;
}
