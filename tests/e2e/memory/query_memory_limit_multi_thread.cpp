// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <future>
#include <thread>

#include <gflags/gflags.h>
#include <iostream>
#include <mgclient.hpp>
#include <utility>

#include "utils/logging.hpp"
#include "utils/timer.hpp"

DEFINE_uint64(bolt_port, 7687, "Bolt port");
DEFINE_bool(multi_db, false, "Run test in multi db environment");

static constexpr int kNumberClients{4};

void Func(std::promise<bool> promise) {
  auto client =
      mg::Client::Connect({.host = "127.0.0.1", .port = static_cast<uint16_t>(FLAGS_bolt_port), .use_ssl = false});
  if (!client) {
    LOG_FATAL("Failed to connect!");
  }

  const auto *create_query =
      "UNWIND range(1, 50000) as u CREATE (n {string: 'Some longer string'}) RETURN n QUERY MEMORY LIMIT 30MB;";
  bool err{false};
  try {
    client->Execute(create_query);
    [[maybe_unused]] auto results = client->FetchAll();
    if (results->empty()) {
      err = true;
    }
  } catch (const mg::TransientException & /*unused*/) {
    spdlog::info("Memgraph is out of memory");
    err = true;
  }
  promise.set_value_at_thread_exit(err);
}

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph E2E Memory Control");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memgraph::logging::RedirectToStderr();

  mg::Client::Init();

  {
    auto client =
        mg::Client::Connect({.host = "127.0.0.1", .port = static_cast<uint16_t>(FLAGS_bolt_port), .use_ssl = false});
    if (!client) {
      LOG_FATAL("Failed to connect!");
    }
    client->Execute("MATCH (n) DETACH DELETE n;");
    client->DiscardAll();

    if (FLAGS_multi_db) {
      client->Execute("CREATE DATABASE clean;");
      client->DiscardAll();
      client->Execute("USE DATABASE clean;");
      client->DiscardAll();
      client->Execute("MATCH (n) DETACH DELETE n;");
      client->DiscardAll();
    }
  }

  std::vector<std::promise<bool>> my_promises;
  std::vector<std::future<bool>> my_futures;
  for (int i = 0; i < 4; i++) {
    my_promises.push_back(std::promise<bool>());
    my_futures.emplace_back(my_promises.back().get_future());
  }

  std::vector<std::thread> my_threads;

  for (int i = 0; i < kNumberClients; i++) {
    my_threads.emplace_back(Func, std::move(my_promises[i]));
  }

  for (int i = 0; i < kNumberClients; i++) {
    auto value = my_futures[i].get();
    std::cout << value << std::endl;
    MG_ASSERT(value, "Error should have happend in thread");
  }

  for (int i = 0; i < kNumberClients; i++) {
    my_threads[i].join();
  }

  return 0;
}
