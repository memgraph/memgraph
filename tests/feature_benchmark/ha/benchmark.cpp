#include <atomic>
#include <chrono>
#include <experimental/optional>
#include <fstream>
#include <thread>

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
DEFINE_int64(query_count, 0, "How many queries should we execute.");
DEFINE_int64(timeout, 60, "How many seconds should the benchmark wait.");
DEFINE_string(output_file, "", "Output file where the results should be.");

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
  google::SetUsageMessage("Memgraph HA benchmark client");
  google::InitGoogleLogging(argv[0]);

  std::atomic<int64_t> query_counter{0};
  std::atomic<bool> timeout_reached{false};
  std::atomic<bool> benchmark_finished{false};

  auto leader_endpoint = GetLeaderEndpoint();
  if (!leader_endpoint) {
    LOG(ERROR) << "Couldn't find Raft cluster leader!";
    return 1;
  }

  // Kickoff a thread that will timeout after FLAGS_timeout seconds
  std::thread timeout_thread_ =
      std::thread([&timeout_reached, &benchmark_finished]() {
        utils::ThreadSetName("BenchTimeout");
        for (int64_t i = 0; i < FLAGS_timeout; ++i) {
          std::this_thread::sleep_for(1s);
          if (benchmark_finished.load()) return;
        }

        timeout_reached.store(true);
      });

  std::vector<std::thread> threads;

  for (int i = 0; i < std::thread::hardware_concurrency(); ++i) {
    threads.emplace_back(
        [endpoint = *leader_endpoint, &timeout_reached, &query_counter]() {
          communication::ClientContext context(FLAGS_use_ssl);
          communication::bolt::Client client(&context);
          client.Connect(endpoint, FLAGS_username, FLAGS_password);

          while (query_counter.load() < FLAGS_query_count) {
            if (timeout_reached.load()) break;

            try {
              client.Execute("CREATE (:Node)", {});
              query_counter.fetch_add(1);
            } catch (const communication::bolt::ClientQueryException &e) {
              LOG(WARNING) << e.what();
              break;
            } catch (const communication::bolt::ClientFatalException &e) {
              LOG(WARNING) << e.what();
              break;
            }
          }
        });
  }

  utils::Timer timer;
  int64_t query_offset = query_counter.load();

  for (auto &t : threads) {
    if (t.joinable()) t.join();
  }

  double duration = timer.Elapsed().count();
  double write_per_second = (query_counter - query_offset) / duration;

  benchmark_finished.store(true);
  if (timeout_thread_.joinable()) timeout_thread_.join();

  std::ofstream output(FLAGS_output_file);
  output << "duration " << duration << std::endl;
  output << "executed_writes " << query_counter << std::endl;
  output << "write_per_second " << write_per_second << std::endl;
  output.close();

  return 0;
}
