#include <chrono>
#include <thread>
#include <vector>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/ha_client.hpp"
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
      client.Execute(fmt::format("CREATE INDEX ON {}", index), {});
      return 0;
    } else if (FLAGS_step == "check") {
      auto result = client.Execute("SHOW INDEX INFO", {});
      auto checker = [&index](const std::vector<Value> &record) {
        if (record.size() != 1) return false;
        return record[0].ValueString() == index;
      };

      // Check that index ":Node(id)" exists
      if (!std::any_of(result.records.begin(), result.records.end(), checker)) {
        LOG(WARNING) << "Missing index!";
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
