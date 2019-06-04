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
DEFINE_int32(create_nodes, 250000, "Number of nodes to be created.");
DEFINE_int32(offset_nodes, 0, "Initial ID of created nodes");
DEFINE_int32(check_nodes, -1, "Number of nodes that should be in the database");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");
DEFINE_string(step, "", "The step to execute (available: create, check)");

using namespace std::chrono_literals;

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  communication::Init();
  try {
    std::vector<io::network::Endpoint> endpoints(FLAGS_cluster_size);
    for (int i = 0; i < FLAGS_cluster_size; ++i)
      endpoints[i] = io::network::Endpoint(FLAGS_address, FLAGS_port + i);

    std::chrono::milliseconds retry_delay(1000);
    communication::ClientContext context(FLAGS_use_ssl);
    communication::bolt::HAClient client(endpoints, &context, FLAGS_username,
                                         FLAGS_password, 60, retry_delay);

    if (FLAGS_step == "create") {
      client.Execute("UNWIND RANGE($start, $stop) AS x CREATE(:Node {id: x})",
                     {{"start", FLAGS_offset_nodes},
                      {"stop", FLAGS_offset_nodes + FLAGS_create_nodes - 1}});
      return 0;
    } else if (FLAGS_step == "check") {
      auto result = client.Execute("MATCH (n) RETURN COUNT(n)", {});
      if (result.records[0][0].ValueInt() != FLAGS_check_nodes) {
        LOG(WARNING) << "Wrong number of nodes! Got " +
                            std::to_string(result.records[0][0].ValueInt()) +
                            ", but expected " +
                            std::to_string(FLAGS_check_nodes);
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
