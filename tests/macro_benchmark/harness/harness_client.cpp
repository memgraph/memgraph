#include <fstream>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common.hpp"
#include "communication/bolt/client.hpp"
#include "communication/bolt/v1/decoder/decoded_value.hpp"
#include "io/network/network_endpoint.hpp"
#include "io/network/socket.hpp"
#include "threading/sync/spinlock.hpp"
#include "utils/algorithm.hpp"
#include "utils/timer.hpp"

using SocketT = io::network::Socket;
using EndpointT = io::network::NetworkEndpoint;
using ClientT = communication::bolt::Client<SocketT>;
using communication::bolt::DecodedValue;

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_string(port, "7687", "Server port");
DEFINE_int32(num_workers, 1, "Number of workers");
DEFINE_string(output, "", "Output file");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");

const int MAX_RETRIES = 1000;

void PrintJsonMetadata(
    std::ostream &os,
    const std::vector<std::map<std::string, DecodedValue>> &metadata) {
  os << "[";
  PrintIterable(os, metadata, ", ", [](auto &stream, const auto &item) {
    PrintJsonDecodedValue(stream, item);
  });
  os << "]";
}

void PrintSummary(
    std::ostream &os, double duration,
    const std::vector<std::map<std::string, DecodedValue>> &metadata) {
  os << "{\"wall_time\": " << duration << ", "
     << "\"metadatas\": ";
  PrintJsonMetadata(os, metadata);
  os << "}";
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  std::string query;
  std::vector<std::thread> threads;

  SpinLock spinlock;
  uint64_t last = 0;
  std::vector<std::string> queries;
  std::vector<std::map<std::string, DecodedValue>> metadata;

  while (std::getline(std::cin, query)) {
    queries.push_back(query);
  }
  metadata.resize(queries.size());

  utils::Timer timer;

  for (int i = 0; i < FLAGS_num_workers; ++i) {
    threads.push_back(std::thread([&]() {
      SocketT socket;
      EndpointT endpoint;

      try {
        endpoint = EndpointT(FLAGS_address, FLAGS_port);
      } catch (const io::network::NetworkEndpointException &e) {
        LOG(FATAL) << "Invalid address or port: " << FLAGS_address << ":"
                   << FLAGS_port;
      }
      if (!socket.Connect(endpoint)) {
        LOG(FATAL) << "Could not connect to: " << FLAGS_address << ":"
                   << FLAGS_port;
      }

      ClientT client(std::move(socket), FLAGS_username, FLAGS_password);

      std::string str;
      while (true) {
        uint64_t pos;
        {
          std::lock_guard<SpinLock> lock(spinlock);
          if (last == queries.size()) {
            break;
          }
          pos = last++;
          str = queries[pos];
        }
        try {
          metadata[pos] =
              ExecuteNTimesTillSuccess(client, str, MAX_RETRIES).metadata;
        } catch (const communication::bolt::ClientQueryException &e) {
          LOG(FATAL) << "Could not execute query '" << str << "' "
                     << MAX_RETRIES << "times";
        }
      }
      client.Close();
    }));
  }

  for (int i = 0; i < FLAGS_num_workers; ++i) {
    threads[i].join();
  }

  auto elapsed = timer.Elapsed();
  double duration = elapsed.count();

  if (FLAGS_output != "") {
    std::ofstream ofile;
    ofile.open(FLAGS_output);
    PrintSummary(ofile, duration, metadata);
  } else {
    PrintSummary(std::cout, duration, metadata);
  }

  return 0;
}
