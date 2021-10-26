// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <atomic>

#include <gflags/gflags.h>

#include "communication/client.hpp"
#include "communication/server.hpp"
#include "utils/exceptions.hpp"

DEFINE_string(server_cert_file, "", "Server certificate file to use.");
DEFINE_string(server_key_file, "", "Server key file to use.");
DEFINE_string(server_ca_file, "", "Server CA file to use.");
DEFINE_bool(server_verify_peer, false, "Set to true to verify the peer.");

DEFINE_string(client_cert_file, "", "Client certificate file to use.");
DEFINE_string(client_key_file, "", "Client key file to use.");

const std::string message = "ssl echo test";

struct EchoData {};

class EchoSession {
 public:
  EchoSession(EchoData *, const io::network::Endpoint &, communication::InputStream *input_stream,
              communication::OutputStream *output_stream)
      : input_stream_(input_stream), output_stream_(output_stream) {}

  void Execute() {
    if (input_stream_->size() < message.size()) return;
    spdlog::info("Server received message.");
    if (!output_stream_->Write(input_stream_->data(), message.size())) {
      throw utils::BasicException("Output stream write failed!");
    }
    spdlog::info("Server sent message.");
    input_stream_->Shift(message.size());
  }

 private:
  communication::InputStream *input_stream_;
  communication::OutputStream *output_stream_;
};

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Initialize the communication stack.
  communication::SSLInit sslInit;

  // Initialize the server.
  EchoData echo_data;
  communication::ServerContext server_context(FLAGS_server_key_file, FLAGS_server_cert_file, FLAGS_server_ca_file,
                                              FLAGS_server_verify_peer);
  communication::Server<EchoSession, EchoData> server({"127.0.0.1", 0}, &echo_data, &server_context, -1, "SSL", 1);
  server.Start();

  // Initialize the client.
  communication::ClientContext client_context(FLAGS_client_key_file, FLAGS_client_cert_file);
  communication::Client client(&client_context);

  // Connect to the server.
  MG_ASSERT(client.Connect(server.endpoint()), "Couldn't connect to server!");

  // Perform echo.
  MG_ASSERT(client.Write(message), "Client couldn't send message!");
  spdlog::info("Client sent message.");
  MG_ASSERT(client.Read(message.size()), "Client couldn't receive message!");
  spdlog::info("Client received message.");
  MG_ASSERT(std::string(reinterpret_cast<const char *>(client.GetData()), message.size()) == message,
            "Received message isn't equal to sent message!");

  // Shutdown the server.
  server.Shutdown();
  server.AwaitShutdown();

  return 0;
}
