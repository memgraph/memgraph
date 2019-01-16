#include <chrono>
#include <iostream>
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
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");

using namespace std::chrono_literals;

void Execute(const std::vector<std::string> &queries) {
  communication::ClientContext context(FLAGS_use_ssl);
  std::experimental::optional<communication::bolt::Client> client;
  communication::bolt::QueryData result;

  for (size_t k = 0; k < queries.size();) {
    if (!client) {
      // Find the leader by executing query on all machines until one responds
      // with success.
      for (int retry = 0; !client && retry < 10; ++retry) {
        for (int i = 0; !client && i < FLAGS_cluster_size; ++i) {
          try {
            client.emplace(&context);

            uint16_t port = FLAGS_port + i;
            io::network::Endpoint endpoint{FLAGS_address, port};

            client->Connect(endpoint, FLAGS_username, FLAGS_password);
            result = client->Execute(queries[k], {});
          } catch (const communication::bolt::ClientQueryException &) {
            // This one is not the leader, continue.
            client = std::experimental::nullopt;
            continue;
          } catch (const communication::bolt::ClientFatalException &) {
            // This one seems to be down, continue.
            client = std::experimental::nullopt;
            continue;
          }
        }

        if (!client) {
          LOG(INFO) << "Couldn't find Raft cluster leader, retrying...";
          std::this_thread::sleep_for(1s);
        }
      }

      if (!client) {
        LOG(ERROR) << "Couldn't find Raft cluster leader.";
        return;
      }

    } else {
      // Try reusing the previous client.
      try {
        result = client->Execute(queries[k], {});
      } catch (const communication::bolt::ClientQueryException &) {
        client = std::experimental::nullopt;
        continue;
      } catch (const communication::bolt::ClientFatalException &e) {
        client = std::experimental::nullopt;
        continue;
      }
    }

    if (result.records.size() > 0) {
      std::cout << "Results: " << std::endl;
      for (auto &record : result.records) {
        std::cout << record << std::endl;
      }
    }

    k += 1;
  }

  return;
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  communication::Init();

  std::vector<std::string> queries;

  std::string line;
  while (std::getline(std::cin, line)) {
    queries.emplace_back(std::move(line));
  }

  Execute(queries);

  return 0;
}
