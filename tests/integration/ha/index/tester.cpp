#include <chrono>
#include <thread>
#include <vector>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/client.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"
#include "utils/timer.hpp"

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_int32(cluster_size, 3, "Size of the raft cluster.");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");
DEFINE_string(step, "", "The step to execute (available: create, check)");

using namespace std::chrono_literals;
using communication::bolt::Value;

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  const std::string index = ":Node(id)";

  communication::Init();

  bool successful = false;
  for (int retry = 0; !successful && retry < 10; ++retry) {
    for (int i = 0; !successful && i < FLAGS_cluster_size; ++i) {
      try {
        communication::ClientContext context(FLAGS_use_ssl);
        communication::bolt::Client client(&context);

        uint16_t port = FLAGS_port + i;
        io::network::Endpoint endpoint{FLAGS_address, port};
        client.Connect(endpoint, FLAGS_username, FLAGS_password);

        if (FLAGS_step == "create") {
          client.Execute(fmt::format("create index on {}", index), {});
          successful = true;

        } else if (FLAGS_step == "check") {
          auto result = client.Execute("show index info", {});

          auto checker = [&index](const std::vector<Value> &record) {
            if (record.size() != 1) return false;
            return record[0].ValueString() == index;
          };

          // Check that index ":Node(id)" exists
          if (!std::any_of(result.records.begin(), result.records.end(),
                           checker)) {
            LOG(WARNING) << "Missing index!";
            return 2;
          }

          successful = true;

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
      LOG(INFO) << "Current Raft cluster leader is " << i;
    }
    if (!successful) {
      LOG(INFO) << "Couldn't find Raft cluster leader, retrying.";
      std::this_thread::sleep_for(1s);
    }
  }

  if (!successful) {
    LOG(WARNING) << "Couldn't find Raft cluster leader.";
    return 1;
  }
  return 0;
}
