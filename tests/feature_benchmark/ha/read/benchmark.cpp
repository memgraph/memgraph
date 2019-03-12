#include <atomic>
#include <chrono>
#include <experimental/optional>
#include <fstream>
#include <random>
#include <thread>

#include <fmt/format.h>
#include <gflags/gflags.h>

#include "communication/bolt/client.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"
#include "utils/flag_validation.hpp"
#include "utils/thread.hpp"
#include "utils/timer.hpp"

using namespace std::literals::chrono_literals;

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_int32(cluster_size, 3, "Size of the raft cluster.");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");
DEFINE_double(duration, 10.0,
              "How long should the client perform reads (seconds)");
DEFINE_string(output_file, "", "Output file where the results should be.");
DEFINE_int32(nodes, 1000, "Number of nodes in DB");
DEFINE_int32(edges, 5000, "Number of edges in DB");

std::experimental::optional<io::network::Endpoint> GetLeaderEndpoint() {
  for (int retry = 0; retry < 10; ++retry) {
    for (int i = 0; i < FLAGS_cluster_size; ++i) {
      try {
        communication::ClientContext context(FLAGS_use_ssl);
        communication::bolt::Client client(&context);

        uint16_t port = FLAGS_port + i;
        io::network::Endpoint endpoint{FLAGS_address, port};

        client.Connect(endpoint, FLAGS_username, FLAGS_password);
        client.Execute("MATCH (n) RETURN n", {});
        client.Close();

        // If we succeeded with the above query, we found the current leader.
        return std::experimental::make_optional(endpoint);

      } catch (const communication::bolt::ClientQueryException &) {
        // This one is not the leader, continue.
        continue;
      } catch (const communication::bolt::ClientFatalException &) {
        // This one seems to be down, continue.
        continue;
      }
    }
    LOG(INFO) << "Couldn't find Raft cluster leader, retrying...";
    std::this_thread::sleep_for(1s);
  }

  return std::experimental::nullopt;
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::SetUsageMessage("Memgraph HA read benchmark client");
  google::InitGoogleLogging(argv[0]);

  std::atomic<int64_t> query_counter{0};

  auto leader_endpoint = GetLeaderEndpoint();
  if (!leader_endpoint) {
    LOG(ERROR) << "Couldn't find Raft cluster leader!";
    return 1;
  }

  // populate the db (random graph with given number of nodes and edges)
  communication::ClientContext context(FLAGS_use_ssl);
  communication::bolt::Client client(&context);
  client.Connect(*leader_endpoint, FLAGS_username, FLAGS_password);
  for (int i = 0; i < FLAGS_nodes; ++i) {
    client.Execute("CREATE (:Node {id:" + std::to_string(i) + "})", {});
  }

  auto seed =
      std::chrono::high_resolution_clock::now().time_since_epoch().count();
  std::mt19937 rng(seed);
  std::uniform_int_distribution<int> dist(0, FLAGS_nodes - 1);

  for (int i = 0; i < FLAGS_edges; ++i) {
    int a = dist(rng), b = dist(rng);
    client.Execute("MATCH (n {id:" + std::to_string(a) + "})," +
                   "      (m {id:" + std::to_string(b) + "})" +
                   "CREATE (n)-[:Edge]->(m);", {});
  }

  const int num_threads = std::thread::hardware_concurrency();
  std::vector<std::thread> threads;
  std::vector<double> thread_duration;
  threads.reserve(num_threads);
  thread_duration.resize(num_threads);

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([i, endpoint = *leader_endpoint, &query_counter,
                          &local_duration = thread_duration[i]]() {
      utils::ThreadSetName(fmt::format("BenchWriter{}", i));
      communication::ClientContext context(FLAGS_use_ssl);
      communication::bolt::Client client(&context);
      client.Connect(endpoint, FLAGS_username, FLAGS_password);

      auto seed =
          std::chrono::high_resolution_clock::now().time_since_epoch().count();
      std::mt19937 rng(seed);
      std::uniform_int_distribution<int> dist(0, FLAGS_nodes - 1);

      utils::Timer t;
      while (true) {
        local_duration = t.Elapsed().count();
        if (local_duration >= FLAGS_duration) break;
        int id = dist(rng);

        try {
          client.Execute("MATCH (n {id:" + std::to_string(id) +
                         "})-[e]->(m) RETURN e, m;", {});
          query_counter.fetch_add(1);
        } catch (const communication::bolt::ClientQueryException &e) {
          LOG(WARNING) << e.what();
          break;
        } catch (const communication::bolt::ClientFatalException &e) {
          LOG(WARNING) << e.what();
          break;
        }
      }

      client.Close();
    });
  }

  for (auto &t : threads) {
    if (t.joinable()) t.join();
  }

  double duration = 0;
  for (auto &d : thread_duration) duration += d;
  duration /= num_threads;

  double read_per_second = query_counter / duration;

  std::ofstream output(FLAGS_output_file);
  output << "duration " << duration << std::endl;
  output << "executed_reads " << query_counter << std::endl;
  output << "read_per_second " << read_per_second << std::endl;
  output.close();

  return 0;
}
