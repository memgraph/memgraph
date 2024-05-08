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

#include <gflags/gflags.h>

#include <iostream>
#include "communication/bolt/client.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"
#include "utils/timer.hpp"

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");
DEFINE_bool(print_records, true, "Set to false to disable printing of records.");

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  memgraph::communication::SSLInit sslInit;

  // TODO: handle endpoint exception
  memgraph::io::network::Endpoint endpoint(memgraph::io::network::ResolveHostname(FLAGS_address), FLAGS_port);

  memgraph::communication::ClientContext context(FLAGS_use_ssl);
  memgraph::communication::bolt::Client client(context);

  client.Connect(endpoint, FLAGS_username, FLAGS_password);

  std::cout << "Memgraph bolt client is connected and running." << std::endl;

  while (true) {
    std::string s;
    std::getline(std::cin, s);
    if (s == "") {
      break;
    }
    try {
      memgraph::utils::Timer t;
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
    } catch (const memgraph::communication::bolt::ClientQueryException &e) {
      std::cout << "Client received exception: " << e.what() << std::endl;
    }
  }

  return 0;
}
