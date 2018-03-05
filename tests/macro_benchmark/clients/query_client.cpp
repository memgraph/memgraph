#include <fstream>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/client.hpp"
#include "communication/bolt/v1/decoder/decoded_value.hpp"
#include "threading/sync/spinlock.hpp"
#include "utils/algorithm.hpp"
#include "utils/string.hpp"
#include "utils/timer.hpp"

#include "bolt_client.hpp"
#include "common.hpp"
//#include "postgres_client.hpp"

DEFINE_string(protocol, "bolt", "Protocol to use (available: bolt, postgres)");
DEFINE_int32(num_workers, 1, "Number of workers");
DEFINE_string(input, "", "Input file");
DEFINE_string(output, "", "Output file");

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 0, "Server port");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_string(database, "", "Database for the database");

using communication::bolt::DecodedValue;

const int MAX_RETRIES = 50;

void PrintJsonMetadata(
    std::ostream &os,
    const std::vector<std::map<std::string, DecodedValue>> &metadata) {
  os << "[";
  utils::PrintIterable(os, metadata, ", ", [](auto &stream, const auto &item) {
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
  os << "}\n";
}

template <typename ClientT>
void ExecuteQueries(const std::vector<std::string> &queries, int num_workers,
                    std::ostream &ostream, std::string &address, uint16_t port,
                    std::string &username, std::string &password,
                    std::string &database) {
  std::vector<std::thread> threads;

  SpinLock spinlock;
  uint64_t last = 0;
  std::vector<std::map<std::string, DecodedValue>> metadata;

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
          metadata[pos] = ExecuteNTimesTillSuccess(client, str, {}, MAX_RETRIES)
                              .first.metadata;
        } catch (const utils::BasicException &e) {
          LOG(FATAL) << "Could not execute query '" << str << "' "
                     << MAX_RETRIES << " times! Error message: " << e.what();
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

  uint16_t port = FLAGS_port;
  if (FLAGS_protocol == "bolt") {
    if (port == 0) port = 7687;
  } else if (FLAGS_protocol == "postgres") {
    if (port == 0) port = 5432;
  }

  while (!istream->eof()) {
    std::vector<std::string> queries;
    std::string query;
    while (std::getline(*istream, query) && utils::Trim(query) != "" &&
           utils::Trim(query) != ";") {
      queries.push_back(query);
    }

    if (FLAGS_protocol == "bolt") {
      ExecuteQueries<BoltClient>(queries, FLAGS_num_workers, *ostream,
                                 FLAGS_address, port, FLAGS_username,
                                 FLAGS_password, FLAGS_database);
    } else if (FLAGS_protocol == "postgres") {
      LOG(FATAL) << "Postgres not yet supported";
      // TODO: Currently libpq is linked dynamically so it is a pain to move
      // harness_client executable to other machines without libpq.
      //    CHECK(FLAGS_username != "") << "Username can't be empty for
      //    postgres!";
      //    CHECK(FLAGS_database != "") << "Database can't be empty for
      //    postgres!";
      //    if (port == "") port = "5432";
      //
      //    using PostgresClientT = postgres::Client;
      //    using PostgresExceptionT = postgres::ClientQueryException;
      //    ExecuteQueries<PostgresClientT, PostgresExceptionT>(
      //        *istream, FLAGS_num_workers, *ostream, FLAGS_address, port,
      //        FLAGS_username, FLAGS_password, FLAGS_database);
    }
  }

  return 0;
}
