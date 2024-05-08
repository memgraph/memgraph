// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <fstream>
#include <mutex>
#include <thread>
#include <vector>

#include <gflags/gflags.h>

#include "communication/init.hpp"
#include "utils/algorithm.hpp"
#include "utils/spin_lock.hpp"
#include "utils/string.hpp"
#include "utils/timer.hpp"

#include <iostream>
#include "common.hpp"

DEFINE_int32(num_workers, 1, "Number of workers");
DEFINE_string(input, "", "Input file");
DEFINE_string(output, "", "Output file");

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");

using memgraph::communication::bolt::Value;

const int MAX_RETRIES = 50;

void PrintJsonMetadata(std::ostream &os, const std::vector<std::map<std::string, Value>> &metadata) {
  os << "[";
  memgraph::utils::PrintIterable(os, metadata, ", ",
                                 [](auto &stream, const auto &item) { PrintJsonValue(stream, item); });
  os << "]";
}

void PrintSummary(std::ostream &os, double duration, const std::vector<std::map<std::string, Value>> &metadata) {
  os << "{\"wall_time\": " << duration << ", "
     << "\"metadatas\": ";
  PrintJsonMetadata(os, metadata);
  os << "}\n";
}

void ExecuteQueries(const std::vector<std::string> &queries, std::ostream &ostream) {
  std::vector<std::thread> threads;

  memgraph::utils::SpinLock spinlock;
  uint64_t last = 0;
  std::vector<std::map<std::string, Value>> metadata;

  metadata.resize(queries.size());

  memgraph::utils::Timer timer;

  for (int i = 0; i < FLAGS_num_workers; ++i) {
    threads.push_back(std::thread([&]() {
      Endpoint endpoint(FLAGS_address, FLAGS_port);
      ClientContext context(FLAGS_use_ssl);
      Client client(context);
      client.Connect(endpoint, FLAGS_username, FLAGS_password);

      std::string str;
      while (true) {
        uint64_t pos;
        {
          auto lock = std::lock_guard{spinlock};
          if (last == queries.size()) {
            break;
          }
          pos = last++;
          str = queries[pos];
        }
        try {
          metadata[pos] = ExecuteNTimesTillSuccess(client, str, {}, MAX_RETRIES).first.metadata;
        } catch (const memgraph::utils::BasicException &e) {
          LOG_FATAL("Could not execute query '{}' {} times! Error message: {}", str, MAX_RETRIES, e.what());
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

  memgraph::communication::SSLInit sslInit;

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
    while (std::getline(*istream, query) && memgraph::utils::Trim(query) != "" && memgraph::utils::Trim(query) != ";") {
      queries.push_back(query);
    }
    ExecuteQueries(queries, *ostream);
  }

  return 0;
}
