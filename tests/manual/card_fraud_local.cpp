#include <atomic>
#include <random>
#include <thread>
#include <vector>

#include "gflags/gflags.h"

#include "distributed_common.hpp"

DEFINE_int32(num_tx_creators, 3, "Number of threads creating transactions");
DEFINE_int32(tx_per_thread, 1000, "Number of transactions each thread creates");

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  Cluster cluster(5);

  cluster.Execute("CREATE INDEX ON :Card(id)");
  cluster.Execute("CREATE INDEX ON :Transaction(id)");
  cluster.Execute("CREATE INDEX ON :Pos(id)");

  int kCardCount = 20000;
  int kPosCount = 20000;

  cluster.Execute("UNWIND range(0, $card_count) AS id CREATE (:Card {id:id})",
                  {{"card_count", kCardCount - 1}});
  cluster.Execute("UNWIND range(0, $pos_count) AS id CREATE (:Pos {id:id})",
                  {{"pos_count", kPosCount - 1}});

  CheckResults(cluster.Execute("MATCH (:Pos) RETURN count(1)"), {{kPosCount}},
               "Failed to create POS");
  CheckResults(cluster.Execute("MATCH (:Card) RETURN count(1)"), {{kCardCount}},
               "Failed to create Cards");

  std::atomic<int> tx_counter{0};
  auto create_tx = [&cluster, kCardCount, kPosCount, &tx_counter](int count) {
    std::mt19937 rand_dev{std::random_device{}()};
    std::uniform_int_distribution<> int_dist;

    auto rint = [&rand_dev, &int_dist](int upper) {
      return int_dist(rand_dev) % upper;
    };

    for (int i = 0; i < count; ++i) {
      try {
        auto res = cluster.Execute(
            "MATCH (p:Pos {id: $pos}), (c:Card {id: $card}) "
            "CREATE (p)<-[:At]-(:Transaction {id : $tx})-[:Using]->(c) "
            "RETURN count(1)",
            {{"pos", rint(kPosCount)},
             {"card", rint(kCardCount)},
             {"tx", tx_counter++}});
        CheckResults(res, {{1}}, "Transaction creation");
      } catch (utils::LockTimeoutException &) {
        --i;
      } catch (mvcc::SerializationError &) {
        --i;
      }
      if (i > 0 && i % 200 == 0)
        LOG(INFO) << "Created " << i << " transactions";
    }
  };

  LOG(INFO) << "Creating " << FLAGS_num_tx_creators * FLAGS_tx_per_thread
            << " transactions in " << FLAGS_num_tx_creators << " threads";
  std::vector<std::thread> tx_creators;
  for (int i = 0; i < FLAGS_num_tx_creators; ++i)
    tx_creators.emplace_back(create_tx, FLAGS_tx_per_thread);
  for (auto &t : tx_creators) t.join();

  CheckResults(cluster.Execute("MATCH (:Transaction) RETURN count(1)"),
               {{FLAGS_num_tx_creators * FLAGS_tx_per_thread}},
               "Failed to create Transactions");

  LOG(INFO) << "Test terminated successfully";
  return 0;
}
