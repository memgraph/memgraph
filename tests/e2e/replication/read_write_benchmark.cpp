#include <chrono>
#include <random>
#include <ranges>
#include <thread>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common.hpp"
#include "utils/thread.hpp"
#include "utils/timer.hpp"

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph E2E Replication Read-write Benchmark");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  const auto database_endpoints =
      mg::e2e::replication::ParseDatabaseEndpoints(FLAGS_database_endpoints);

  mg::Client::Init();

  {
    auto client = mg::e2e::replication::Connect(database_endpoints[0]);
    client->Execute("MATCH (n) DETACH DELETE n;");
    client->DiscardAll();
    client->Execute("CREATE INDEX ON :Node(id);");
    client->DiscardAll();

    // Sleep a bit so the index get replicated.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    for (const auto &database_endpoint : database_endpoints) {
      auto client = mg::e2e::replication::Connect(database_endpoint);
      client->Execute("SHOW INDEX INFO;");
      if (auto data = client->FetchAll()) {
        auto label_name = (*data)[0][1].ValueString();
        auto property_name = (*data)[0][2].ValueString();
        if (label_name != "Node" || property_name != "id") {
          LOG(FATAL) << database_endpoint.host << ":" << database_endpoint.port
                     << " does NOT hava valid indexes created.";
        }
      } else {
        LOG(FATAL) << "Unable to get INDEX INFO from " << database_endpoint.host
                   << ":" << database_endpoint.port;
      }
    }
    LOG(INFO) << "All indexes are in-place.";

    for (int i = 0; i < FLAGS_nodes; ++i) {
      client->Execute("CREATE (:Node {id:" + std::to_string(i) + "});");
      client->DiscardAll();
    }
    mg::e2e::replication::IntGenerator edge_generator("EdgeCreateGenerator", 0,
                                                      FLAGS_nodes - 1);
    for (int i = 0; i < FLAGS_edges; ++i) {
      client->Execute("MATCH (n {id:" + std::to_string(edge_generator.Next()) +
                      "}), (m {id:" + std::to_string(edge_generator.Next()) +
                      "}) CREATE (n)-[:Edge]->(m);");
      client->DiscardAll();
    }
  }

  {
    const int num_threads = std::thread::hardware_concurrency();
    std::atomic<int64_t> query_counter{0};
    std::vector<std::thread> threads;
    std::vector<double> thread_duration;
    threads.reserve(num_threads);
    thread_duration.resize(num_threads);

    for (int i = 0; i < num_threads; ++i) {
      const auto &database_endpoint =
          database_endpoints[i % database_endpoints.size()];
      threads.emplace_back([i, &database_endpoint, &query_counter,
                            &local_duration = thread_duration[i]]() {
        utils::ThreadSetName(fmt::format("BenchWriter{}", i));
        auto client = mg::e2e::replication::Connect(database_endpoint);
        mg::e2e::replication::IntGenerator node_generator(
            fmt::format("NodeReadGenerator {}", i), 0, FLAGS_nodes - 1);
        utils::Timer t;

        while (true) {
          local_duration = t.Elapsed().count();
          if (local_duration >= FLAGS_reads_duration_limit) break;
          try {
            client->Execute(
                "MATCH (n {id:" + std::to_string(node_generator.Next()) +
                "})-[e]->(m) RETURN e, m;");
            client->DiscardAll();
            query_counter.fetch_add(1);
          } catch (const std::exception &e) {
            LOG(FATAL) << e.what();
            break;
          }
        }
      });
    }

    for (auto &t : threads) {
      if (t.joinable()) t.join();
    }

    double all_duration = 0;
    for (auto &d : thread_duration) all_duration += d;
    double per_thread_duration = all_duration / num_threads;
    double reads_per_second = query_counter / per_thread_duration;

    LOG(INFO) << "Total duration: " << all_duration;
    LOG(INFO) << "Query count: " << query_counter;
    LOG(INFO) << "Reads per second: " << reads_per_second;
  }

  {
    auto client = mg::e2e::replication::Connect(database_endpoints[0]);
    client->Execute("DROP INDEX ON :Node(id);");
    client->DiscardAll();
    // Sleep a bit so the index get replicated.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    for (const auto &database_endpoint : database_endpoints) {
      auto client = mg::e2e::replication::Connect(database_endpoint);
      client->Execute("SHOW INDEX INFO;");
      if (const auto data = client->FetchAll()) {
        if ((*data).size() != 0) {
          LOG(FATAL) << database_endpoint.host << ":" << database_endpoint.port
                     << " still have some indexes.";
        }
      } else {
        LOG(FATAL) << "Unable to get INDEX INFO from " << database_endpoint.host
                   << ":" << database_endpoint.port;
      }
    }
    LOG(INFO) << "All indexes were deleted.";
  }

  mg::Client::Finalize();

  return 0;
}
