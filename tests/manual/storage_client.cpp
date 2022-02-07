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
#include <iostream>
#include <ratio>
#include <thread>

#include <folly/Executor.h>
#include <folly/Unit.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/executors/ThreadedExecutor.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <folly/init/Init.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <thrift/lib/cpp2/async/ClientStreamBridge.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>
#include <thrift/lib/cpp2/async/RocketClientChannel.h>

#include "interface/gen-cpp2/StorageAsyncClient.h"
#include "interface/gen-cpp2/storage_types.h"
#include "interface/storage.hpp"

DEFINE_string(host, "127.0.0.1", "Storage Server host");
DEFINE_int32(port, 7779, "Storage Server port");

using interface::storage::CreateVerticesRequest;
using interface::storage::StorageAsyncClient;
using interface::storage::Value;

int main(int argc, char *argv[]) {
  FLAGS_logtostderr = true;
  folly::init(&argc, &argv);
  auto threadFactory = std::make_shared<folly::NamedThreadFactory>("io-thread");
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(8, std::move(threadFactory));

  // Create a Thrift client.
  std::unique_ptr<StorageAsyncClient> client{nullptr};

  ioThreadPool->getEventBase()->runInEventBaseThreadAndWait([&ioThreadPool, &client]() {
    try {
      LOG(INFO) << "Doing... ";
      auto socket = folly::AsyncSocket::newSocket(ioThreadPool->getEventBase(), FLAGS_host, FLAGS_port);
      auto channel = apache::thrift::HeaderClientChannel::newChannel(std::move(socket));
      // channel->setTimeout(0);

      client = std::make_unique<StorageAsyncClient>(std::move(channel));
      LOG(INFO) << "Done... ";
    } catch (const std::exception &) {
    }
  });
  LOG(INFO) << "Start... ";

  // interface::storage::Result result {};
  interface::storage::CreateVerticesRequest request {};
  request.property_name_map()->emplace(1, "prop1");
  request.labels_name_map_ref()->emplace(2, "label2");
  auto &new_vertex = request.new_vertices_ref()->emplace_back();
  new_vertex.label_ids_ref()->push_back(2);
  interface::storage::Value prop_value {};
  prop_value.set_string_v("value");
  new_vertex.properties_ref()->emplace(1, std::move(prop_value));
  // handler.crateVertices(result, std::move(request));

  LOG(INFO) << "Sending message...";
  client->future_createVertices(request)
      .via(ioThreadPool->getEventBase())
      .then([](folly::Try<interface::storage::Result> &&reply) {
        if (reply.hasException()) {
          std::cout << "FAILED: " << reply.exception().get_exception()->what() << std::endl;

        } else {
          std::cout << "SUCCESS\n";
        }
      });

  LOG(INFO) << "Sent!";
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  };
  LOG(INFO) << "Received!";

  if (client != nullptr) {
    ioThreadPool->getEventBase()->runInEventBaseThreadAndWait(
        [client = std::move(client)]() mutable { delete client.release(); });
    client = nullptr;
  }
}
