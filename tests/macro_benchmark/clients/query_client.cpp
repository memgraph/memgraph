#include <fstream>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "utils/algorithm.hpp"
#include "utils/string.hpp"
#include "utils/thread/sync.hpp"
#include "utils/timer.hpp"

#include "common.hpp"

DEFINE_int32(num_workers, 1, "Number of workers");
DEFINE_string(input, "", "Input file");
DEFINE_string(output, "", "Output file");

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");

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

void ExecuteQueries(const std::vector<std::string> &queries,
                    std::ostream &ostream) {
  std::vector<std::thread> threads;

  utils::SpinLock spinlock;
  uint64_t last = 0;
  std::vector<std::map<std::string, DecodedValue>> metadata;

  metadata.resize(queries.size());

  utils::Timer timer;

  for (int i = 0; i < FLAGS_num_workers; ++i) {
    threads.push_back(std::thread([&]() {
      Endpoint endpoint(FLAGS_address, FLAGS_port);
      Client client;
      if (!client.Connect(endpoint, FLAGS_username, FLAGS_password)) {
        LOG(FATAL) << "Couldn't connect to " << endpoint;
      }

      std::string str;
      while (true) {
        uint64_t pos;
        {
          std::lock_guard<utils::SpinLock> lock(spinlock);
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

  for (int i = 0; i < FLAGS_num_workers; ++i) {
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

  while (!istream->eof()) {
    std::vector<std::string> queries;
    std::string query;
    while (std::getline(*istream, query) && utils::Trim(query) != "" &&
           utils::Trim(query) != ";") {
      queries.push_back(query);
    }
    ExecuteQueries(queries, *ostream);
  }

  return 0;
}
