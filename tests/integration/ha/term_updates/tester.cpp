#include <chrono>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/ha_client.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"
#include "utils/timer.hpp"

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_int32(cluster_size, 3, "Size of the raft cluster.");
DEFINE_int32(expected_results, -1, "Number of expected nodes.");
DEFINE_int32(num_retries, 20, "Number of (leader) execution retries.");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");

DEFINE_string(step, "create", "The step to execute (available: create, count)");

using namespace std::chrono_literals;

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  communication::Init();

  try {
    std::vector<io::network::Endpoint> endpoints;
    for (int i = 0; i < FLAGS_cluster_size; ++i) {
      uint16_t port = FLAGS_port + i;
      io::network::Endpoint endpoint{FLAGS_address, port};
      endpoints.push_back(endpoint);
    }

    std::chrono::milliseconds retry_delay(1000);
    communication::ClientContext context(FLAGS_use_ssl);
    communication::bolt::HAClient client(endpoints, &context, FLAGS_username,
                                         FLAGS_password, FLAGS_num_retries,
                                         retry_delay);

    if (FLAGS_step == "create") {
      client.Execute("create (:Node)", {});
      return 0;

    } else if (FLAGS_step == "count") {
      auto result = client.Execute("match (n) return n", {});

      if (result.records.size() != FLAGS_expected_results) {
        LOG(WARNING) << "Unexpected number of nodes: "
                     << "expected " << FLAGS_expected_results
                     << ", got " << result.records.size();
        return 2;
      }
      return 0;

    } else if (FLAGS_step == "find_leader") {
      std::cout << client.GetLeaderId();
      return 0;

    } else {
      LOG(FATAL) << "Unexpected client step!";
    }
  } catch (const communication::bolt::ClientQueryException &) {
    LOG(WARNING) << "There was some transient error during query execution.";
  } catch (const communication::bolt::ClientFatalException &) {
    LOG(WARNING) << "Failed to communicate with the leader.";
  } catch (const utils::BasicException &e) {
    LOG(WARNING) << "Error while executing query.";
  }

  return 1;
}
