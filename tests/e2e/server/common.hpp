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

#include <spdlog/spdlog.h>

#include <mgclient.hpp>

#include "utils/logging.hpp"

void EstablishConnection(const uint16_t bolt_port, const bool use_ssl) {
  spdlog::info("Testing successfull connection from one client");
  mg::Client::Init();
  auto client = mg::Client::Connect({.host = "127.0.0.1", .port = bolt_port, .use_ssl = use_ssl});
  MG_ASSERT(client, "Failed to connect!");
}

void EstablishMultipleConnections(const uint16_t bolt_port, const bool use_ssl) {
  spdlog::info("Testing successfull connection from multiple clients");
  mg::Client::Init();
  auto client1 = mg::Client::Connect({.host = "127.0.0.1", .port = bolt_port, .use_ssl = use_ssl});
  auto client2 = mg::Client::Connect({.host = "127.0.0.1", .port = bolt_port, .use_ssl = use_ssl});
  auto client3 = mg::Client::Connect({.host = "127.0.0.1", .port = bolt_port, .use_ssl = use_ssl});

  MG_ASSERT(client1, "Failed to connect!");
  MG_ASSERT(client2, "Failed to connect!");
  MG_ASSERT(client3, "Failed to connect!");
}
