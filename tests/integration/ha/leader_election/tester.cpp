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
DEFINE_int32(has_majority, 0, "Should we be able to elect the leader.");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");

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
                                         FLAGS_password, 15, retry_delay);

    auto leader = client.GetLeaderId();
    if (!FLAGS_has_majority) {
      LOG(WARNING)
          << "The majority of cluster is dead but we have elected server "
          << std::to_string(leader) << " as a leader";
      return 1;
    } else {
      LOG(INFO) << "Server " << std::to_string(leader)
                << " was successfully elected as a leader";
      return 0;
    }

  } catch (const communication::bolt::ClientFatalException &e) {
    if (FLAGS_has_majority) {
      LOG(WARNING)
          << "The majority of cluster is alive but the leader was not elected.";
      return 1;
    } else {
      LOG(INFO)
          << "The has_majority of cluster is dead and no leader was elected.";
      return 0;
    }
  } catch (const communication::bolt::ClientQueryException &e) {
    LOG(WARNING)
        << "Transient error while executing query. (eg. mistyped query, etc.)\n"
        << e.what();
  } catch (const utils::BasicException &e) {
    LOG(WARNING) << "Error while executing query\n" << e.what();
  }

  // The test wasn't successfull
  return 1;
}
