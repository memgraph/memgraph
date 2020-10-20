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
DEFINE_int32(node_count, -1, "Expected number of nodes in the database.");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");
DEFINE_string(step, "", "The step to execute (available: create, count)");

using namespace std::chrono_literals;

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  communication::SSLInit sslInit;

  try {
    std::vector<io::network::Endpoint> endpoints(FLAGS_cluster_size);
    for (int i = 0; i < FLAGS_cluster_size; ++i)
      endpoints[i] = io::network::Endpoint(FLAGS_address, FLAGS_port + i);

    std::chrono::milliseconds retry_delay(1000);
    communication::ClientContext context(FLAGS_use_ssl);
    communication::bolt::HAClient client(endpoints, &context, FLAGS_username,
                                         FLAGS_password, 25, retry_delay);

    if (FLAGS_step == "create") {
      client.Execute("create (:Node {id: $id})",
                     {{"id", FLAGS_node_count + 1}});
      return 0;
    } else if (FLAGS_step == "count") {
      auto result = client.Execute("match (n) return n", {});
      if (result.records.size() != FLAGS_node_count) {
        LOG(WARNING) << "Missing data: expected " << FLAGS_node_count
                     << ", got " << result.records.size();
        return 2;
      }
      return 0;
    } else {
      LOG(FATAL) << "Unexpected client step!";
    }
  } catch (const communication::bolt::ClientQueryException &e) {
    LOG(WARNING)
        << "Transient error while executing query. (eg. mistyped query, etc.)\n"
        << e.what();
  } catch (const communication::bolt::ClientFatalException &e) {
    LOG(WARNING) << "Couldn't connect to server\n" << e.what();
  } catch (const utils::BasicException &e) {
    LOG(WARNING) << "Error while executing query\n" << e.what();
  }

  // The test wasn't successfull
  return 1;
}
