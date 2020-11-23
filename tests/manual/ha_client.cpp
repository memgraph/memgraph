#include <chrono>
#include <iostream>
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
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");
DEFINE_bool(print_records, true,
            "Set to false to disable printing of records.");
DEFINE_int32(num_retries, 3,
             "Number of retries for each operation (execute/connect).");
DEFINE_int32(retry_delay_ms, 1000, "Delay before retrying in ms.");

// NOLINTNEXTLINE(bugprone-exception-escape)
int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  communication::SSLInit sslInit;

  std::vector<io::network::Endpoint> endpoints;
  endpoints.reserve(FLAGS_cluster_size);
  for (int i = 0; i < FLAGS_cluster_size; ++i) {
    endpoints.push_back({FLAGS_address, static_cast<uint16_t>(FLAGS_port + i)});
  }

  communication::ClientContext context(FLAGS_use_ssl);
  communication::bolt::HAClient client(
      endpoints, &context, FLAGS_username, FLAGS_password, FLAGS_num_retries,
      std::chrono::milliseconds(FLAGS_retry_delay_ms));

  while (true) {
    std::string s;
    std::getline(std::cin, s);
    if (s == "") {
      break;
    }
    try {
      utils::Timer t;
      auto ret = client.Execute(s, {});
      auto elapsed = t.Elapsed().count();
      std::cout << "Wall time:\n    " << elapsed << std::endl;

      std::cout << "Fields:" << std::endl;
      for (auto &field : ret.fields) {
        std::cout << "    " << field << std::endl;
      }

      if (FLAGS_print_records) {
        std::cout << "Records:" << std::endl;
        for (int i = 0; i < static_cast<int>(ret.records.size()); ++i) {
          std::cout << "    " << i << std::endl;
          for (auto &value : ret.records[i]) {
            std::cout << "        " << value << std::endl;
          }
        }
      }

      std::cout << "Metadata:" << std::endl;
      for (auto &data : ret.metadata) {
        std::cout << "    " << data.first << " : " << data.second << std::endl;
      }
    } catch (const communication::bolt::ClientQueryException &e) {
      std::cout << "Client received exception: " << e.what() << std::endl;
    }
  }

  return 0;
}
