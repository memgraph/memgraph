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

#include <string>

#include <folly/init/Init.h>
#include <folly/portability/GFlags.h>
#include <proxygen/httpserver/HTTPServerOptions.h>
#include <spdlog/common.h>
#include <thrift/lib/cpp/concurrency/ThreadManager.h>
#include <thrift/lib/cpp/thrift_config.h>
#include <thrift/lib/cpp2/server/ThriftProcessor.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>
#include <thrift/lib/cpp2/transport/rocket/server/RocketRoutingHandler.h>

#include "query/exceptions.hpp"
#include "query/frontend/opencypher/generated/MemgraphCypher.h"
#include "query/frontend/opencypher/generated/MemgraphCypherLexer.h"
#include "query/interpret/eval.hpp"
#include "storage_service.hpp"

using apache::thrift::RocketRoutingHandler;
using apache::thrift::ThriftServer;
using apache::thrift::ThriftServerAsyncProcessorFactory;
using manual::storage::StorageServiceHandler;

std::unique_ptr<RocketRoutingHandler> createRoutingHandler(std::shared_ptr<ThriftServer> server) {
  return std::make_unique<RocketRoutingHandler>(*server);
}

template <typename ServiceHandler>
std::shared_ptr<ThriftServer> createServer(memgraph::storage::Storage &db, int32_t port) {
  auto handler = std::make_shared<ServiceHandler>(db);
  auto proc_factory = std::make_shared<ThriftServerAsyncProcessorFactory<ServiceHandler>>(handler);
  auto server = std::make_shared<ThriftServer>();
  server->setPort(port);
  server->setInterface(handler);
  return server;
}

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::trace);
  // Main storage and execution engines initialization
  memgraph::storage::Config db_config{
      .gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = std::chrono::seconds(1000)},
      .items = {.properties_on_edges = true},
      .durability = {.storage_directory = "data",
                     .recover_on_startup = false,
                     .snapshot_wal_mode = memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT,
                     .snapshot_interval = std::chrono::seconds(15),
                     .snapshot_retention_count = 2,
                     .wal_file_size_kibibytes = 20480,
                     .wal_file_flush_every_n_tx = 1,
                     .snapshot_on_exit = true},
      .transaction = {.isolation_level = memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION}};

  memgraph::storage::Storage db(db_config);

  folly::init(&argc, &argv);

  auto storage_server = createServer<StorageServiceHandler>(db, 7779);
  std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager(
      apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(8));
  threadManager->setNamePrefix("executor");
  threadManager->start();
  storage_server->setThreadManager(threadManager);
  std::cout << "alma\n";
  LOG(ERROR) << "Storage Server running on port: " << 7779;
  storage_server->serve();

  return 0;
}