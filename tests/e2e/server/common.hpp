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

#include <chrono>
#include <functional>
#include <thread>

#include <spdlog/spdlog.h>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/system/detail/error_code.hpp>
#include <mgclient.hpp>

#include "utils/logging.hpp"

inline void OnTimeoutExpiration(const boost::system::error_code &ec) {
  // Timer was not cancelled, take necessary action.
  MG_ASSERT(!!ec, "Connection timeout");
}

inline void EstablishConnection(const uint16_t bolt_port, const bool use_ssl) {
  spdlog::info("Testing successful connection from one client");
  mg::Client::Init();

  boost::asio::io_context ioc;
  boost::asio::steady_timer timer(ioc, std::chrono::seconds(5));
  timer.async_wait(std::bind_front(&OnTimeoutExpiration));
  std::jthread bg_thread([&ioc]() { ioc.run(); });

  auto client = mg::Client::Connect({.host = "127.0.0.1", .port = bolt_port, .use_ssl = use_ssl});
  MG_ASSERT(client, "Failed to connect!");
  timer.cancel();
}

inline void EstablishMultipleConnections(const uint16_t bolt_port, const bool use_ssl) {
  spdlog::info("Testing successful connection from multiple clients");
  mg::Client::Init();

  boost::asio::io_context ioc;
  boost::asio::steady_timer timer(ioc, std::chrono::seconds(5));
  timer.async_wait(std::bind_front(&OnTimeoutExpiration));
  std::jthread bg_thread([&ioc]() { ioc.run(); });

  auto client1 = mg::Client::Connect({.host = "127.0.0.1", .port = bolt_port, .use_ssl = use_ssl});
  auto client2 = mg::Client::Connect({.host = "127.0.0.1", .port = bolt_port, .use_ssl = use_ssl});
  auto client3 = mg::Client::Connect({.host = "127.0.0.1", .port = bolt_port, .use_ssl = use_ssl});

  MG_ASSERT(client1, "Failed to connect!");
  MG_ASSERT(client2, "Failed to connect!");
  MG_ASSERT(client3, "Failed to connect!");
  timer.cancel();
}
