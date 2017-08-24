#include <fstream>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/client.hpp"
#include "io/network/network_endpoint.hpp"
#include "io/network/socket.hpp"
#include "threading/sync/spinlock.hpp"
#include "utils/algorithm.hpp"
#include "utils/timer.hpp"

using SocketT = io::network::Socket;
using EndpointT = io::network::NetworkEndpoint;
using ClientT = communication::bolt::Client<SocketT>;
using DecodedValueT = communication::bolt::DecodedValue;

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_string(port, "7687", "Server port");
DEFINE_uint64(num_workers, 1, "Number of workers");
DEFINE_string(output, "", "Output file");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");

const uint64_t MAX_RETRIES = 1000;

void PrintJsonDecodedValue(std::ostream &os, const DecodedValueT &value) {
  switch (value.type()) {
    case DecodedValueT::Type::Null:
      os << "null";
      break;
    case DecodedValueT::Type::Bool:
      os << (value.ValueBool() ? "true" : "false");
      break;
    case DecodedValueT::Type::Int:
      os << value.ValueInt();
      break;
    case DecodedValueT::Type::Double:
      os << value.ValueDouble();
      break;
    case DecodedValueT::Type::String:
      os << "\"" << value.ValueString() << "\"";
      break;
    case DecodedValueT::Type::List:
      os << "[";
      PrintIterable(os, value.ValueList(), ", ",
                    [](auto &stream, const auto &item) {
                      PrintJsonDecodedValue(stream, item);
                    });
      os << "]";
      break;
    case DecodedValueT::Type::Map:
      os << "{";
      PrintIterable(os, value.ValueMap(), ", ",
                    [](auto &stream, const auto &pair) {
                      PrintJsonDecodedValue(stream, {pair.first});
                      stream << ": ";
                      PrintJsonDecodedValue(stream, pair.second);
                    });
      os << "}";
      break;
    default:
      std::terminate();
  }
}

void PrintJsonMetadata(
    std::ostream &os,
    const std::vector<std::map<std::string, DecodedValueT>> &metadata) {
  os << "[";
  PrintIterable(os, metadata, ", ", [](auto &stream, const auto &item) {
    PrintJsonDecodedValue(stream, item);
  });
  os << "]";
}

void PrintSummary(
    std::ostream &os, double duration,
    const std::vector<std::map<std::string, DecodedValueT>> &metadata) {
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

  SpinLock mutex;
  uint64_t last = 0;
  std::vector<std::string> queries;
  std::vector<std::map<std::string, DecodedValueT>> metadata;

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
        std::terminate();
      }
      if (!socket.Connect(endpoint)) {
        std::terminate();
      }

      ClientT client(std::move(socket), FLAGS_username, FLAGS_password);

      uint64_t pos, i;
      std::string str;
      while (true) {
        {
          std::lock_guard<SpinLock> lock(mutex);
          if (last == queries.size()) {
            break;
          }
          pos = last++;
          str = queries[pos];
        }
        for (i = 0; i < MAX_RETRIES; ++i) {
          try {
            auto ret = client.Execute(str, {});
            std::lock_guard<SpinLock> lock(mutex);
            metadata[pos] = ret.metadata;
            break;
          } catch (const communication::bolt::ClientQueryException &e) {
          }
        }
        if (i == MAX_RETRIES) {
          std::terminate();
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
