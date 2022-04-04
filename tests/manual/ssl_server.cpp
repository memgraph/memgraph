// Copyright 2022 Memgraph Ltd.
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

#include "communication/server.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 54321, "Server port");
DEFINE_string(cert_file, "", "Certificate file to use.");
DEFINE_string(key_file, "", "Key file to use.");
DEFINE_string(ca_file, "", "CA file to use.");
DEFINE_bool(verify_peer, false, "Set to true to verify the peer.");

struct EchoData {
  std::atomic<bool> alive{true};
};

class EchoSession {
 public:
  EchoSession(EchoData *data, const memgraph::io::network::Endpoint &,
              memgraph::communication::InputStream *input_stream, memgraph::communication::OutputStream *output_stream)
      : data_(data), input_stream_(input_stream), output_stream_(output_stream) {}

  void Execute() {
    if (input_stream_->size() < 2) return;
    const uint8_t *data = input_stream_->data();
    uint16_t size = *reinterpret_cast<const uint16_t *>(input_stream_->data());
    input_stream_->Resize(size + 2);
    if (input_stream_->size() < size + 2) return;
    if (size == 0) {
      spdlog::info("Server received EOF message");
      data_->alive.store(false);
      return;
    }
    spdlog::info("Server received '{}'", std::string(reinterpret_cast<const char *>(data + 2), size));
    if (!output_stream_->Write(data + 2, size)) {
      throw memgraph::utils::BasicException("Output stream write failed!");
    }
    input_stream_->Shift(size + 2);
  }

 private:
  EchoData *data_;
  memgraph::communication::InputStream *input_stream_;
  memgraph::communication::OutputStream *output_stream_;
};

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  memgraph::communication::SSLInit sslInit;

  // Initialize the server.
  EchoData echo_data;
  memgraph::io::network::Endpoint endpoint(FLAGS_address, FLAGS_port);
  memgraph::communication::ServerContext context(FLAGS_key_file, FLAGS_cert_file, FLAGS_ca_file, FLAGS_verify_peer);
  memgraph::communication::Server<EchoSession, EchoData> server(endpoint, &echo_data, &context, -1, "SSL", 1);
  server.Start();

  while (echo_data.alive) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  server.Shutdown();
  server.AwaitShutdown();

  return 0;
}
