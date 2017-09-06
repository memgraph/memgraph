#include <fstream>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/v1/decoder/decoded_value.hpp"
#include "threading/sync/spinlock.hpp"
#include "utils/algorithm.hpp"
#include "utils/timer.hpp"

#include "bolt_client.hpp"
#include "common.hpp"
#include "postgres_client.hpp"

DEFINE_string(protocol, "bolt", "Protocol to use (available: bolt, postgres)");
DEFINE_int32(num_workers, 1, "Number of workers");
DEFINE_string(input, "", "Input file");
DEFINE_string(output, "", "Output file");

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_string(port, "", "Server port");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_string(database, "", "Database for the database");

using communication::bolt::DecodedValue;

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

template <typename ClientT, typename ExceptionT>
void ExecuteQueries(std::istream &istream, int num_workers,
                    std::ostream &ostream, std::string &address,
                    std::string &port, std::string &username,
                    std::string &password, std::string &database) {
  std::string query;
  std::vector<std::thread> threads;

  SpinLock spinlock;
  uint64_t last = 0;
  std::vector<std::string> queries;
  std::vector<std::map<std::string, DecodedValue>> metadata;

  while (std::getline(istream, query)) {
    queries.push_back(query);
  }
  metadata.resize(queries.size());

  utils::Timer timer;

  for (int i = 0; i < num_workers; ++i) {
    threads.push_back(std::thread([&]() {
      ClientT client(address, port, username, password, database);

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
          metadata[pos] = ExecuteNTimesTillSuccess<ClientT, ExceptionT>(
                              client, str, MAX_RETRIES)
                              .metadata;
        } catch (const ExceptionT &e) {
          LOG(FATAL) << "Could not execute query '" << str << "' "
                     << MAX_RETRIES << "times";
        }
      }
      client.Close();
    }));
  }

  for (int i = 0; i < num_workers; ++i) {
    threads[i].join();
  }

  auto elapsed = timer.Elapsed();
  double duration = elapsed.count();

  PrintSummary(ostream, duration, metadata);
}

using BoltClientT = BoltClient;
using BoltExceptionT = communication::bolt::ClientQueryException;

using PostgresClientT = postgres::Client;
using PostgresExceptionT = postgres::ClientQueryException;

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  std::ifstream ifile;
  std::istream *istream{&std::cin};

  std::ofstream ofile;
  std::ostream *ostream{&std::cout};

  if (FLAGS_input != "") {
    ifile.open(FLAGS_input);
    istream = &ifile;
  }

  if (FLAGS_output != "") {
    ofile.open(FLAGS_output);
    ostream = &ofile;
  }

  std::string port = FLAGS_port;
  if (FLAGS_protocol == "bolt") {
    if (port == "") port = "7687";
    ExecuteQueries<BoltClientT, BoltExceptionT>(
        *istream, FLAGS_num_workers, *ostream, FLAGS_address, port,
        FLAGS_username, FLAGS_password, FLAGS_database);
  } else if (FLAGS_protocol == "postgres") {
    permanent_assert(FLAGS_username != "",
                     "Username can't be empty for postgres!");
    permanent_assert(FLAGS_database != "",
                     "Database can't be empty for postgres!");
    if (port == "") port = "5432";
    ExecuteQueries<PostgresClientT, PostgresExceptionT>(
        *istream, FLAGS_num_workers, *ostream, FLAGS_address, port,
        FLAGS_username, FLAGS_password, FLAGS_database);
  }

  return 0;
}
