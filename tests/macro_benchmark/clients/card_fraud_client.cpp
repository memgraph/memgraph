#include <memory>
#include <random>
#include <vector>

#include "gflags/gflags.h"

#include "long_running_common.hpp"
#include "stats/stats.hpp"

// TODO(mtomic): this sucks but I don't know a different way to make it work
#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/serialization/export.hpp"
BOOST_CLASS_EXPORT(stats::StatsReq);
BOOST_CLASS_EXPORT(stats::StatsRes);
BOOST_CLASS_EXPORT(stats::BatchStatsReq);
BOOST_CLASS_EXPORT(stats::BatchStatsRes);


class CardFraudClient : public TestClient {
 public:
  CardFraudClient(int id, int num_pos, nlohmann::json config)
      : TestClient(), rg_(id), num_pos_(num_pos), config_(config) {}

 private:
  std::mt19937 rg_;
  int num_pos_;
  nlohmann::json config_;

  auto GetFraudulentTransactions() {
    return Execute(
        "MATCH (t:Transaction {fraud_reported: true}) "
        "RETURN t.id as id",
        {});
  }

  auto GetCompromisedPos() {
    return Execute(
        "MATCH (t:Transaction {fraud_reported: true})-[:Using]->(:Card)"
        "<-[:Using]-(:Transaction)-[:At]->(p:Pos) "
        "WITH p.id as pos, count(t) as connected_frauds "
        "WHERE connected_frauds > 1 "
        "RETURN pos, connected_frauds ORDER BY connected_frauds DESC",
        {});
  }

  auto ResolvePos(int id) {
    return Execute(
        "MATCH (p:Pos {id: $id}) "
        "SET p.compromised = false "
        "WITH p MATCH (p)--(t:Transaction)--(c:Card) "
        "SET t.fraud_reported = false, c.compromised = false",
        {{"id", id}});
  }

  auto CompromisePos(int id) {
    return Execute(
        "MATCH (p:Pos {id: $id}) "
        "SET p.compromised = true "
        "WITH p MATCH (p)--(t:Transaction)--(c:Card) "
        "SET t.fraud_reported = false, c.compromised = true",
        {{"id", id}});
  }

 public:
  virtual void Step() override {
    if (config_["scenario"] == "read_only") {
      std::uniform_int_distribution<int> dist(0, 1);
      if (dist(rg_)) {
        GetFraudulentTransactions();
      } else {
        GetCompromisedPos();
      }
    } else if (config_["scenario"] == "read_write") {
      std::uniform_int_distribution<int> dist(0, num_pos_ - 1);
      int pos_id = dist(rg_);
      CompromisePos(pos_id);
      GetFraudulentTransactions();
      ResolvePos(pos_id);
    } else {
      LOG(FATAL) << "Should not get here!";
    }
  }
};

int64_t NumPos(BoltClient &client) {
  auto result = ExecuteNTimesTillSuccess(
      client, "MATCH (n :Pos) RETURN COUNT(n) as cnt;", {}, MAX_RETRIES);
  return result.records[0][0].ValueInt();
}

void CreateIndex(BoltClient &client, const std::string &label,
                 const std::string &property) {
  LOG(INFO) << fmt::format("Creating indexes for :{}({})...", label, property);
  ExecuteNTimesTillSuccess(
      client, fmt::format("CREATE INDEX ON :{}({});", label, property), {},
      MAX_RETRIES);
  try {
    LOG(INFO) << fmt::format("Trying to sync indexes...");
    ExecuteNTimesTillSuccess(client, "CALL db.awaitIndexes(14400);", {},
                             MAX_RETRIES);
  } catch (utils::BasicException &e) {
    LOG(WARNING) << "Index sync failed: " << e.what();
  }
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  InitStatsLogging();

  nlohmann::json config;
  std::cin >> config;

  BoltClient client(FLAGS_address, FLAGS_port, FLAGS_username, FLAGS_password);
  int num_pos = NumPos(client);
  CreateIndex(client, "Card", "id");
  CreateIndex(client, "Pos", "id");
  CreateIndex(client, "Transaction", "fraud_reported");

  LOG(INFO) << "Done building indexes.";

  std::vector<std::unique_ptr<TestClient>> clients;
  for (int i = 0; i < FLAGS_num_workers; ++i) {
    clients.emplace_back(std::make_unique<CardFraudClient>(i, num_pos, config));
  }

  RunMultithreadedTest(clients);

  StopStatsLogging();

  return 0;
}
