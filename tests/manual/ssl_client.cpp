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
#include "communication/client.hpp"
#include "io/network/endpoint.hpp"
#include "utils/logging.hpp"
#include "utils/timer.hpp"

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 54321, "Server port");
DEFINE_string(cert_file, "", "Certificate file to use.");
DEFINE_string(key_file, "", "Key file to use.");

bool EchoMessage(memgraph::communication::Client &client, const std::string &data) {
  uint16_t size = data.size();
  if (!client.Write(reinterpret_cast<const uint8_t *>(&size), sizeof(size))) {
    spdlog::warn("Couldn't send data size!");
    return false;
  }
  if (!client.Write(data)) {
    spdlog::warn("Couldn't send data!");
    return false;
  }

  client.ClearData();
  if (!client.Read(size)) {
    spdlog::warn("Couldn't receive data!");
    return false;
  }
  if (std::string(reinterpret_cast<const char *>(client.GetData()), size) != data) {
    spdlog::warn("Received data isn't equal to sent data!");
    return false;
  }
  return true;
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  memgraph::communication::SSLInit sslInit;

  memgraph::io::network::Endpoint endpoint(FLAGS_address, FLAGS_port);

  memgraph::communication::ClientContext context(FLAGS_key_file, FLAGS_cert_file);
  memgraph::communication::Client client(&context);

  if (!client.Connect(endpoint)) return 1;

  bool success = true;
  while (true) {
    std::string s;
    std::getline(std::cin, s);
    if (s == "") break;
    if (!EchoMessage(client, s)) {
      success = false;
      break;
    }
  }

  // Send server shutdown signal. The call will fail, we don't care.
  EchoMessage(client, "");

  return success ? 0 : 1;
}
