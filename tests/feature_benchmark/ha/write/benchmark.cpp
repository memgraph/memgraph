#include <atomic>
#include <chrono>
#include <fstream>
#include <optional>
#include <thread>

#include <fmt/format.h>
#include <gflags/gflags.h>

#include "communication/bolt/ha_client.hpp"
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
              "How long should the client perform writes (seconds)");
DEFINE_string(output_file, "", "Output file where the results should be.");

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::SetUsageMessage("Memgraph HA write benchmark client");
  google::InitGoogleLogging(argv[0]);

  std::atomic<int64_t> query_counter{0};

  std::vector<io::network::Endpoint> endpoints;
  for (int i = 0; i < FLAGS_cluster_size; ++i) {
    uint16_t port = FLAGS_port + i;
      io::network::Endpoint endpoint{FLAGS_address, port};
      endpoints.push_back(endpoint);
  }

  const int num_threads = std::thread::hardware_concurrency();
  std::vector<std::thread> threads;
  std::vector<double> thread_duration;
  threads.reserve(num_threads);
  thread_duration.resize(num_threads);

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([i, endpoints = endpoints, &query_counter,
                          &local_duration = thread_duration[i]]() {
      utils::ThreadSetName(fmt::format("BenchWriter{}", i));
      communication::ClientContext context(FLAGS_use_ssl);
      std::chrono::milliseconds retry_delay(1000);
      communication::bolt::HAClient client(endpoints, &context, FLAGS_username,
                                           FLAGS_password, 10, retry_delay);

      utils::Timer t;
      while (true) {
        local_duration = t.Elapsed().count();
        if (local_duration >= FLAGS_duration) break;

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

  for (auto &t : threads) {
    if (t.joinable()) t.join();
  }

  double duration = 0;
  for (auto &d : thread_duration) duration += d;
  duration /= num_threads;

  double write_per_second = query_counter / duration;

  std::ofstream output(FLAGS_output_file);
  output << "duration " << duration << std::endl;
  output << "executed_writes " << query_counter << std::endl;
  output << "write_per_second " << write_per_second << std::endl;
  output.close();

  return 0;
}
