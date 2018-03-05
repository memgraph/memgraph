#include <memory>
#include <random>
#include <vector>

#include "gflags/gflags.h"

#include "long_running_common.hpp"
#include "stats/stats.hpp"
#include "stats/stats_rpc_messages.hpp"

// TODO(mtomic): this sucks but I don't know a different way to make it work
#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/serialization/export.hpp"
BOOST_CLASS_EXPORT(stats::StatsReq);
BOOST_CLASS_EXPORT(stats::StatsRes);
BOOST_CLASS_EXPORT(stats::BatchStatsReq);
BOOST_CLASS_EXPORT(stats::BatchStatsRes);

std::atomic<int> num_pos;
std::atomic<int> num_cards;
std::atomic<int> num_transactions;

DEFINE_string(config, "", "test config");

enum class Role { WORKER, ANALYTIC };

class CardFraudClient : public TestClient {
 public:
  CardFraudClient(int id, nlohmann::json config, Role role = Role::WORKER)
      : TestClient(), rg_(id), role_(role), config_(config) {}

 private:
  std::mt19937 rg_;
  Role role_;
  nlohmann::json config_;

  auto HeavyRead() {
    return Execute(
        "MATCH (t:Transaction {fraud_reported: true}) "
        "WITH COLLECT(t.id) as ids "
        "RETURN head(ids)",
        {}, "HeavyRead");
  }

  auto GetFraudulentTransactions() {
    return Execute(
        "MATCH (t:Transaction {fraud_reported: true}) "
        "RETURN t.id as id",
        {}, "GetFraudulentTransactions");
  }

  auto GetCompromisedPos(int pos_limit) {
    return Execute(
        "MATCH (t:Transaction {fraud_reported: true})-[:Using]->(:Card)"
        "<-[:Using]-(:Transaction)-[:At]->(p:Pos) "
        "WITH p.id as pos, count(t) as connected_frauds "
        "WHERE connected_frauds > 1 "
        "RETURN pos, connected_frauds "
        "ORDER BY connected_frauds DESC LIMIT $pos_limit",
        {{"pos_limit", pos_limit}}, "GetCompromisedPos");
  }

  auto GetCompromisedPosInc(int pos_limit) {
    return Execute(
        "MATCH (p:Pos) "
        "RETURN p, p.connected_frauds "
        "ORDER BY p.connected_frauds DESC "
        "LIMIT $pos_limit",
        {{"pos_limit", pos_limit}}, "GetCompromisedPosInc");
  }

  auto ResolvePos(int id) {
    return Execute(
        "MATCH (p:Pos {id: $id}) "
        "SET p.compromised = false "
        "WITH p MATCH (p)--(t:Transaction)--(c:Card) "
        "SET t.fraud_reported = false, c.compromised = false",
        {{"id", id}}, "ResolvePos");
  }

  auto GetTransaction(int id) {
    return Execute("MATCH (t:Transaction {id: $id}) RETURN (t)", {{"id", id}},
                   "GetTransaction");
  }

  auto TepsQuery() {
    auto result = Execute("MATCH (u)--(v) RETURN count(1)", {}, "TepsQuery");
    DCHECK(result && result->records[0][0].ValueInt() == num_transactions * 2);
  }

  auto CompromisePos(int id) {
    return Execute(
        "MATCH (p:Pos {id: $id}) "
        "SET p.compromised = true "
        "WITH p MATCH (p)--(t:Transaction)--(c:Card) "
        "SET t.fraud_reported = false, c.compromised = true",
        {{"id", id}}, "CompromisePos");
  }
  auto CreateTransaction(int pos_id, int card_id, int tx_id, bool is_fraud) {
    return Execute(
        "MATCH (p:Pos {id: $pos_id}), (c:Card {id: $card_id}) "
        "CREATE (c)<-[:Using]-(t:Transaction {id: $tx_id, fraud_reported: "
        "$is_fraud})-[:At]->(p)",
        {{"pos_id", pos_id},
         {"card_id", card_id},
         {"tx_id", tx_id},
         {"is_fraud", is_fraud}},
        "CreateTransaction");
  }

  auto CreateTransactionWithoutEdge(int pos_id, int card_id, int tx_id,
                                    bool is_fraud) {
    return Execute(
        "MATCH (p:Pos {id: $pos_id}), (c:Card {id: $card_id}) "
        "CREATE (t:Transaction {id: $tx_id, fraud_reported: false})",
        {{"pos_id", pos_id},
         {"card_id", card_id},
         {"tx_id", tx_id},
         {"is_fraud", is_fraud}},
        "CreateTransactionWithoutEdge");
  }

  auto UpdateFraudScores(int tx_id) {
    return Execute(
        "MATCH (t:Transaction {id: "
        "$tx_id})-[:Using]->(:Card)<-[:Using]-(:Transaction)-[:At]->(p:Pos) "
        "SET p.connected_frauds = p.connected_frauds + 1",
        {{"tx_id", tx_id}}, "UpdateFraudScores");
  }

  int UniformInt(int a, int b) {
    std::uniform_int_distribution<int> dist(a, b);
    return dist(rg_);
  }

  double UniformDouble(double a, double b) {
    std::uniform_real_distribution<double> dist(a, b);
    return dist(rg_);
  }

 public:
  virtual void Step() override {
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
      CreateTransaction(UniformInt(0, num_pos - 1),
                        UniformInt(0, num_cards - 1), num_transactions++,
                        false);
      return;
    }

    /*
     * - fraud scores are calculated as transactions are added
     * - no deletions
     */
    if (FLAGS_scenario == "strata_v1") {
      if (role_ == Role::ANALYTIC) {
        std::this_thread::sleep_for(std::chrono::milliseconds(
            config_["analytic_query_interval"].get<int>()));
        GetCompromisedPosInc(config_["analytic_query_pos_limit"].get<int>());
        return;
      }

      if (role_ == Role::WORKER) {
        bool is_fraud =
            UniformDouble(0, 1) < config_["fraud_probability"].get<double>();

        int pos_id = UniformInt(0, num_pos - 1);
        int pos_worker = pos_id / config_["pos_per_worker"].get<int>();

        int card_worker = pos_worker;
        bool hop =
            UniformDouble(0, 1) < config_["hop_probability"].get<double>();

        if (hop) {
          card_worker = UniformInt(0, config_["num_workers"].get<int>() - 2);
          if (card_worker >= pos_worker) {
            ++card_worker;
          }
        }

        int card_id = card_worker * config_["cards_per_worker"].get<int>() +

                      UniformInt(0, config_["cards_per_worker"].get<int>() - 1);

        int tx_id = num_transactions++;
        CreateTransaction(pos_id, card_id, tx_id, is_fraud);
        if (is_fraud) {
          UpdateFraudScores(tx_id);
        }
      }
      return;
    }

    if (FLAGS_scenario == "strata_v2") {
      LOG(FATAL) << "Not yet implemented!";
      return;
    }

    if (FLAGS_scenario == "strata_v3") {
      LOG(FATAL) << "Not yet implemented!";
      return;
    }

    LOG(FATAL) << "Should not get here: unknown scenario!";
  }
};

int64_t NumNodesWithLabel(BoltClient &client, std::string label) {
  std::string query = fmt::format("MATCH (u :{}) RETURN COUNT(u)", label);
  auto result = ExecuteNTimesTillSuccess(client, query, {}, MAX_RETRIES);
  return result.first.records[0][0].ValueInt();
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

nlohmann::json LoadConfig() {
  nlohmann::json config;
  if (FLAGS_config != "") {
    LOG(INFO) << "Loading config from: " << FLAGS_config;
    std::ifstream is(FLAGS_config);
    is >> config;
  } else {
    LOG(INFO) << "No test config provided";
  }
  return config;
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  stats::InitStatsLogging(
      fmt::format("client.long_running.{}.{}", FLAGS_group, FLAGS_scenario));

  BoltClient client(FLAGS_address, FLAGS_port, FLAGS_username, FLAGS_password);

  num_pos.store(NumNodesWithLabel(client, "Pos"));
  num_cards.store(NumNodesWithLabel(client, "Card"));
  num_transactions.store(NumNodesWithLabel(client, "Transaction"));

  CreateIndex(client, "Pos", "id");
  CreateIndex(client, "Card", "id");
  CreateIndex(client, "Transaction", "fraud_reported");
  CreateIndex(client, "Transaction", "id");
  LOG(INFO) << "Done building indexes.";

  client.Close();

  auto config = LoadConfig();

  std::vector<std::unique_ptr<TestClient>> clients;
  if (FLAGS_scenario == "strata_v1") {
    CHECK(num_pos == config["num_workers"].get<int>() *
                         config["pos_per_worker"].get<int>())
        << "Wrong number of POS per worker";
    CHECK(num_cards == config["num_workers"].get<int>() *
                           config["cards_per_worker"].get<int>())
        << "Wrong number of cards per worker";
    for (int i = 0; i < FLAGS_num_workers - 1; ++i) {
      clients.emplace_back(std::make_unique<CardFraudClient>(i, config));
    }
    clients.emplace_back(std::make_unique<CardFraudClient>(
        FLAGS_num_workers - 1, config, Role::ANALYTIC));
  } else {
    for (int i = 0; i < FLAGS_num_workers; ++i) {
      clients.emplace_back(std::make_unique<CardFraudClient>(i, config));
    }
  }

  RunMultithreadedTest(clients);

  stats::StopStatsLogging();

  return 0;
}
