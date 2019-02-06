#include <chrono>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/client.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"
#include "utils/timer.hpp"

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_int32(cluster_size, 3, "Size of the raft cluster.");
DEFINE_int32(expected_results, -1, "Number of expected nodes.");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");

DEFINE_string(step, "", "The step to execute (available: create, count)");

using namespace std::chrono_literals;

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  communication::Init();

  bool successfull = false;
  for (int retry = 0; !successfull && retry < 10; ++retry) {
    for (int i = 0; !successfull && i < FLAGS_cluster_size; ++i) {
      try {
        communication::ClientContext context(FLAGS_use_ssl);
        communication::bolt::Client client(&context);

        uint16_t port = FLAGS_port + i;
        io::network::Endpoint endpoint{FLAGS_address, port};
        client.Connect(endpoint, FLAGS_username, FLAGS_password);

        if (FLAGS_step == "create") {
          client.Execute("create (:Node)", {});
          successfull = true;

        } else if (FLAGS_step == "count") {
          auto result = client.Execute("match (n) return n", {});

          if (result.records.size() != FLAGS_expected_results) {
            LOG(WARNING) << "Missing data: expected " << FLAGS_expected_results
                         << ", got " << result.records.size();
            return 2;
          }

          successfull = true;

        } else {
          LOG(FATAL) << "Unexpected client step!";
        }
      } catch (const communication::bolt::ClientQueryException &) {
        // This one is not the leader, continue.
        continue;
      } catch (const communication::bolt::ClientFatalException &) {
        // This one seems to be down, continue.
        continue;
      }
    }
    if (!successfull) {
      LOG(INFO) << "Couldn't find Raft cluster leader, retrying.";
      std::this_thread::sleep_for(1s);
    }
  }

  if (!successfull) {
    LOG(WARNING) << "Couldn't find Raft cluster leader.";
    return 1;
  }
  return 0;
}
