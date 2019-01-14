#include <atomic>
#include <chrono>
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

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::SetUsageMessage("Memgraph HA benchmark client");
  google::InitGoogleLogging(argv[0]);

  int64_t query_counter = 0;
  std::atomic<bool> timeout_reached{false};
  std::atomic<bool> benchmark_finished{false};

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

  double duration = 0;
  double write_per_second = 0;

  bool successful = false;
  for (int retry = 0; !successful && retry < 10; ++retry) {
    for (int i = 0; !successful && i < FLAGS_cluster_size; ++i) {
      try {
        communication::ClientContext context(FLAGS_use_ssl);
        communication::bolt::Client client(&context);

        uint16_t port = FLAGS_port + i;
        io::network::Endpoint endpoint{FLAGS_address, port};
        client.Connect(endpoint, FLAGS_username, FLAGS_password);

        utils::Timer timer;
        for (int k = 0; k < FLAGS_query_count; ++k) {
          client.Execute("CREATE (:Node)", {});
          query_counter++;

          if (timeout_reached.load()) break;
        }

        duration = timer.Elapsed().count();
        successful = true;

      } catch (const communication::bolt::ClientQueryException &) {
        // This one is not the leader, continue.
        continue;
      } catch (const communication::bolt::ClientFatalException &) {
        // This one seems to be down, continue.
        continue;
      }

      if (timeout_reached.load()) break;
    }

    if (timeout_reached.load()) break;
    if (!successful) {
      LOG(INFO) << "Couldn't find Raft cluster leader, retrying...";
      std::this_thread::sleep_for(1s);
    }
  }

  benchmark_finished.store(true);
  if (timeout_thread_.joinable()) timeout_thread_.join();

  if (successful) {
    write_per_second = query_counter / duration;
  }

  std::ofstream output(FLAGS_output_file);
  output << "duration " << duration << std::endl;
  output << "executed_writes " << query_counter << std::endl;
  output << "write_per_second " << write_per_second << std::endl;
  output.close();

  if (!successful) return 1;
  return 0;
}
