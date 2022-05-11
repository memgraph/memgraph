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

#include <unistd.h>
#include <chrono>
#include <cstddef>

#include <gflags/gflags.h>
#include <spdlog/spdlog.h>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/system/detail/error_code.hpp>
#include <mgclient.hpp>

#include "common.hpp"
#include "utils/logging.hpp"

DEFINE_uint64(bolt_port, 7687, "Bolt port");

void EstablishSSLConnectionToNonSSLServer(const auto bolt_port) {
  spdlog::info("Testing that connection fails when connecting to non SSL server while using SSL");
  mg::Client::Init();

  boost::asio::io_context ioc;
  boost::asio::steady_timer timer(ioc, std::chrono::seconds(5));
  timer.async_wait(std::bind_front(&OnTimeoutExpiration));
  std::jthread bg_thread([&ioc]() { ioc.run(); });

  auto client = mg::Client::Connect({.host = "127.0.0.1", .port = bolt_port, .use_ssl = true});

  MG_ASSERT(client == nullptr, "Connection not refused when connecting with SSL turned on to a non SSL server!");
}

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph E2E server connection!");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  MG_ASSERT(FLAGS_bolt_port != 0);
  memgraph::logging::RedirectToStderr();

  const auto bolt_port = static_cast<uint16_t>(FLAGS_bolt_port);

  EstablishConnection(bolt_port, false);
  EstablishMultipleConnections(bolt_port, false);
  EstablishSSLConnectionToNonSSLServer(bolt_port);

  return 0;
}
