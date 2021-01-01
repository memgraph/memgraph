#include <chrono>
#include <random>
#include <ranges>
#include <thread>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "mgclient.hpp"
#include "utils/string.hpp"
#include "utils/thread.hpp"
#include "utils/timer.hpp"

struct DatabaseEndpoint {
  std::string host;
  uint16_t port;
};

DEFINE_string(database_endpoints,
              "127.0.0.1:7687,127.0.0.1:7688,127.0.0.1:7689",
              "An array of database endspoints. Each endpoint is separated by "
              "comma. Within each endpoint, colon separates host and port. Use "
              "IPv4 addresses as hosts. First endpoint represents main "
              "replication instance.");
DEFINE_string(username, "", "Database username.");
DEFINE_string(password, "", "Database password.");
DEFINE_bool(use_ssl, false, "Use SSL connection.");
DEFINE_int32(nodes, 1000, "Number of nodes in DB.");
DEFINE_int32(edges, 5000, "Number of edges in DB.");
DEFINE_double(reads_duration_limit, 10.0,
              "How long should the client perform reads (seconds)");

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph E2E Replication Benchmark Test");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  const auto db_endpoints_strs = utils::Split(FLAGS_database_endpoints, ",");
  std::vector<DatabaseEndpoint> database_endpoints;
  for (const auto &db_endpoint_str : db_endpoints_strs) {
    const auto &hps = utils::Split(db_endpoint_str, ":");
    database_endpoints.emplace_back(DatabaseEndpoint{
        .host = hps[0], .port = static_cast<uint16_t>(std::stoi(hps[1]))});
  }
  const int database_cluster_size = database_endpoints.size();

  mg::Client::Init();

  {
    mg::Client::Params params;
    params.host = database_endpoints[0].host;
    params.port = database_endpoints[0].port;
    params.use_ssl = FLAGS_use_ssl;
    auto client = mg::Client::Connect(params);
    if (!client) {
      LOG(FATAL) << "Failed to connect!";
    }
    client->Execute("MATCH (n) DETACH DELETE n;");
    client->DiscardAll();
    client->Execute("CREATE INDEX ON :Node(id);");
    client->DiscardAll();

    // TODO(gitbuda): Add more threads in a configurable way and measure
    // writes.
    for (int i = 0; i < FLAGS_nodes; ++i) {
      client->Execute("CREATE (:Node {id:" + std::to_string(i) + "});");
      client->DiscardAll();
    }
    auto seed =
        std::chrono::high_resolution_clock::now().time_since_epoch().count();
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int> dist(0, FLAGS_nodes - 1);
    for (int i = 0; i < FLAGS_edges; ++i) {
      int a = dist(rng), b = dist(rng);
      client->Execute("MATCH (n {id:" + std::to_string(a) + "})," +
                      "      (m {id:" + std::to_string(b) + "}) " +
                      "CREATE (n)-[:Edge]->(m);");
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
          database_endpoints[i % database_cluster_size];
      threads.emplace_back([i, &database_endpoint, &query_counter,
                            &local_duration = thread_duration[i]]() {
        utils::ThreadSetName(fmt::format("BenchWriter{}", i));
        mg::Client::Params params;
        params.host = database_endpoint.host;
        params.port = database_endpoint.port;
        params.use_ssl = FLAGS_use_ssl;
        auto client = mg::Client::Connect(params);
        if (!client) {
          LOG(FATAL) << "Failed to connect!";
        }
        auto seed = std::chrono::high_resolution_clock::now()
                        .time_since_epoch()
                        .count();
        std::mt19937 rng(seed);
        std::uniform_int_distribution<int> dist(0, FLAGS_nodes - 1);

        utils::Timer t;
        while (true) {
          local_duration = t.Elapsed().count();
          if (local_duration >= FLAGS_reads_duration_limit) break;
          int id = dist(rng);

          try {
            client->Execute("MATCH (n {id:" + std::to_string(id) +
                            "})-[e]->(m) RETURN e, m;");
            // TODO(gitbuda): Replace with FetchAll (implement it).
            client->DiscardAll();
            query_counter.fetch_add(1);
          } catch (const std::exception &e) {
            LOG(WARNING) << e.what();
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

  mg::Client::Finalize();

  return 0;
}
