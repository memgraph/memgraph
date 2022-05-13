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
#include <cstdint>
#include <iostream>
#include <optional>
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
#include "storage/v2/view.hpp"
#include "utils/logging.hpp"

DEFINE_string(host, "127.0.0.1", "Storage Server host");
DEFINE_int32(port, 7779, "Storage Server port");

using interface::storage::CreateVerticesRequest;
using interface::storage::StorageAsyncClient;
using interface::storage::Value;
using interface::storage::Vertex;

void Print(std::ostream &os, const Value &value) {
  switch (value.getType()) {
    case Value::Type::null_v:
      os << "<null>";
      break;
    case Value::Type::bool_v:
      os << value.get_bool_v();
      break;
    case Value::Type::int_v:
      os << value.get_int_v();
      break;
    case Value::Type::string_v:
      os << value.get_string_v();
      break;
    default:
      os << "UNKNOWN TYPE";
      break;
  }
}

std::optional<int64_t> ScanVertices(const std::shared_ptr<StorageAsyncClient> &client, int64_t transaction_id,
                                    const std::optional<int64_t> &start_id,
                                    const std::optional<std::vector<std::string>> &props_to_return,
                                    const std::optional<int64_t> &limit,
                                    const memgraph::storage::View view = memgraph::storage::View::NEW) {
  interface::storage::ScanVerticesRequest req;
  req.transaction_id_ref() = transaction_id;
  req.view_ref() = view;
  if (limit.has_value()) {
    req.limit_ref() = limit.value();
  }
  if (start_id.has_value()) {
    req.start_id_ref() = start_id.value();
  }

  if (props_to_return.has_value()) {
    req.props_to_return_ref() = props_to_return.value();
  }

  return client->future_scanVertices(req)
      .then(
          [&props_to_return](folly::Try<interface::storage::ScanVerticesResponse> &&result) -> std::optional<int64_t> {
            if (result.hasException()) {
              LOG(INFO) << "FAILED: " << result.exception().get_exception()->what() << std::endl;
              return std::nullopt;
            }
            if (!*result->result_ref()->success()) {
              throw std::runtime_error("scan vertices failed");
            }

            const auto &response = result.value();
            MG_ASSERT(props_to_return.has_value() != response.property_name_map_ref().has_value());
            if (props_to_return.has_value()) {
              const auto values_list_ref = response.values_ref()->listed_ref()->properties_ref();
              const auto &prop_names = props_to_return.value();
              if (values_list_ref->empty()) {
                return std::nullopt;
              }
              MG_ASSERT(values_list_ref->at(0).size() == prop_names.size() + 1UL);

              for (const auto &values : *values_list_ref) {
                auto value_it = values.begin();
                std::cout << "Vertex(";
                Print(std::cout, *value_it++);
                std::cout << ')';
                auto name_it = prop_names.begin();
                while (value_it != values.end()) {
                  std::cout << "\n\t" << *name_it << ": ";
                  Print(std::cout, *value_it);
                  ++value_it;
                  ++name_it;
                }
                std::cout << std::endl;
              }
            } else {
              const auto values_list_ref = response.values_ref()->mapped_ref()->properties();
              const auto mapped_prop_names_ref = response.property_name_map_ref();

              for (const auto &values_map : *values_list_ref) {
                const auto inner_values_map_ref = values_map.values_map_ref();
                std::cout << "Vertex(";
                Print(std::cout, inner_values_map_ref->at(1));
                std ::cout << ')';
                for (const auto &[prop_id, value] : *inner_values_map_ref) {
                  if (prop_id != 1) {
                    std::cout << "\n\t" << mapped_prop_names_ref->at(prop_id) << ": ";
                    Print(std::cout, value);
                  }
                }
                std::cout << std::endl;
              }
            }

            if (response.next_start_id_ref().has_value()) {
              return *response.get_next_start_id();
            }
            return std::nullopt;
          })
      .get();
}

void CreateVertex(const std::shared_ptr<StorageAsyncClient> &client, std::vector<std::string_view> labels,
                  std::vector<std::string_view> property_names) {
  interface::storage::CreateVerticesRequest request {};
  auto &new_vertex = request.new_vertices_ref()->emplace_back();

  int prop_count = 1;
  for (const auto prop : property_names) {
    request.property_name_map()->emplace(prop_count, prop);
    interface::storage::Value prop_value {};
    prop_value.set_int_v(prop_count);
    new_vertex.properties_ref()->emplace(prop_count, std::move(prop_value));
    prop_count++;
  }
  int label_count = 1;
  for (const auto label : labels) {
    request.labels_name_map_ref()->emplace(label_count, label);
    new_vertex.label_ids_ref()->push_back(label_count);
    label_count++;
  }

  client->future_startTransaction()
      .then([client, request = std::move(request)](folly::Try<int64_t> &&result) mutable {
        if (result.hasException()) {
          LOG(INFO) << "FAILED1: " << result.exception().get_exception()->what() << std::endl;
          return folly::makeFuture<interface::storage::Result>(std::runtime_error("failed to start transaction"));
        }
        const auto transaction_id = result.value();
        request.transaction_id_ref() = transaction_id;
        LOG(INFO) << "Sending message...";
        return client->future_createVertices(request) /*.via(evb)*/.then(
            [transaction_id, client](folly::Try<interface::storage::Result> &&reply) {
              LOG(INFO) << "Closing transaction";
              if (reply.hasException()) {
                LOG(INFO) << "FAILED2: " << reply.exception().get_exception()->what() << std::endl;
                return client->future_abortTransaction(transaction_id).then([](folly::Try<void> &&reply) {
                  return folly::makeFuture<interface::storage::Result>(std::runtime_error("vertex creation failed"));
                });
              } else {
                LOG(INFO) << "SUCCESS\n";
                return client->future_commitTransaction(transaction_id);
              }
            });
      })
      .get();
}

void CreateEdge(const std::shared_ptr<StorageAsyncClient> &client, uint64_t src, uint64_t dest, std::string type,
                std::vector<std::string_view> property_names) {
  interface::storage::CreateEdgesRequest request {};
  auto &new_edge = request.new_edges_ref()->emplace_back();
  new_edge.src_ref().emplace(src);
  new_edge.dest_ref().emplace(dest);
  new_edge.type_ref()->name_ref().emplace(type);

  int prop_count = 1;
  for (const auto prop : property_names) {
    request.property_name_map()->emplace(prop_count, prop);
    interface::storage::Value prop_value {};
    prop_value.set_int_v(prop_count);
    new_edge.properties_ref()->emplace(prop_count, std::move(prop_value));
    prop_count++;
  }

  client->future_startTransaction()
      .then([client, request = std::move(request)](folly::Try<int64_t> &&result) mutable {
        if (result.hasException()) {
          LOG(INFO) << "FAILED1: " << result.exception().get_exception()->what() << std::endl;
          return folly::makeFuture<interface::storage::Result>(std::runtime_error("failed to start transaction"));
        }
        const auto transaction_id = result.value();
        request.transaction_id_ref() = transaction_id;
        LOG(INFO) << "Sending message...";

        return client->future_createEdges(request).then(
            [transaction_id, client](folly::Try<interface::storage::Result> &&reply) {
              LOG(INFO) << "Closing transaction";
              if (reply.hasException()) {
                LOG(INFO) << "FAILED2: " << reply.exception().get_exception()->what() << std::endl;
                return client->future_abortTransaction(transaction_id).then([](folly::Try<void> &&reply) {
                  return folly::makeFuture<interface::storage::Result>(std::runtime_error("edge creation failed"));
                });
              }
              LOG(INFO) << "SUCCESS\n";
              return client->future_commitTransaction(transaction_id);
            });
      })
      .get();
}

void UpdateVerticesProperties(const std::shared_ptr<StorageAsyncClient> &client, std::vector<uint64_t> vertices_id,
                              std::vector<std::string_view> labels, std::vector<std::string_view> property_names) {
  interface::storage::UpdateVerticesRequest request {};
  request.added_labels_ref()->assign(labels.cbegin(), labels.cend());
  request.vertices_id_ref()->assign(vertices_id.cbegin(), vertices_id.cend());

  int prop_count = 10;
  for (const auto prop : property_names) {
    interface::storage::UpdateProperty update_prop {};
    update_prop.name = prop;
    update_prop.value.set_int_v(prop_count);
    request.updated_props_ref()->emplace_back(update_prop);
    prop_count++;
  }

  client->future_startTransaction()
      .then([client, request = std::move(request)](folly::Try<int64_t> &&result) mutable {
        if (result.hasException()) {
          LOG(INFO) << "FAILED1: " << result.exception().get_exception()->what() << std::endl;
          return folly::makeFuture<interface::storage::Result>(std::runtime_error("failed to start transaction"));
        }
        const auto transaction_id = result.value();
        request.transaction_id_ref() = transaction_id;
        LOG(INFO) << "Sending message...";

        return client->future_updateVertices(request).then(
            [transaction_id, client](folly::Try<interface::storage::Result> &&reply) {
              LOG(INFO) << "Closing transaction";
              if (reply.hasException()) {
                LOG(INFO) << "FAILED2: " << reply.exception().get_exception()->what() << std::endl;
                return client->future_abortTransaction(transaction_id).then([](folly::Try<void> &&reply) {
                  return folly::makeFuture<interface::storage::Result>(std::runtime_error("updating vertices failed"));
                });
              }
              LOG(INFO) << "SUCCESS\n";
              return client->future_commitTransaction(transaction_id);
            });
      })
      .get();
}

int main(int argc, char *argv[]) {
  FLAGS_logtostderr = true;
  folly::init(&argc, &argv);
  auto threadFactory = std::make_shared<folly::NamedThreadFactory>("io-thread");
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(8, std::move(threadFactory));
  auto *evb = ioThreadPool->getEventBase();

  folly::AsyncSocket::UniquePtr socket_ptr;
  evb->runImmediatelyOrRunInEventBaseThreadAndWait(
      [&socket_ptr, evb]() { socket_ptr = folly::AsyncSocket::newSocket(evb, FLAGS_host, FLAGS_port); });
  auto headerClientChannel = apache::thrift::HeaderClientChannel::newChannel(std::move(socket_ptr));

  std::shared_ptr<StorageAsyncClient> client(
      new StorageAsyncClient(std::move(headerClientChannel)),
      [evb](StorageAsyncClient *p) { evb->runImmediatelyOrRunInEventBaseThreadAndWait([p] { delete p; }); });
  LOG(INFO) << "Start... ";

  CreateVertex(client, {"label1", "label2"}, {"proop", "prooop2"});
  CreateVertex(client, {"label1", "label2"}, {"proop", "prooop3"});
  CreateVertex(client, {"label1", "label2"}, {"proop", "prooop4"});

  CreateVertex(client, {"label1", "label3"}, {"proop", "prooop2"});
  CreateVertex(client, {"label1", "label4"}, {"proop", "prooop3"});
  CreateVertex(client, {"label1", "label5"}, {"proop", "prooop4"});

  // TODO: Until we have hash-based vertex id implemented this is temporary
  CreateEdge(client, 0, 1, "type1", {"proop", "prooop2"});
  CreateEdge(client, 1, 2, "type1", {"proop", "prooop2"});
  CreateEdge(client, 2, 3, "type5", {});
  // This should fail
  // CreateEdge(client, 12121212, 2121212121, "type5", {});

  UpdateVerticesProperties(client, {0, 1, 2}, {}, {"proop", "new_prop"});
  UpdateVerticesProperties(client, {0}, {"el_label"}, {"proop", "new_new_prop"});

  const auto transaction_id = client->future_startTransaction().get();
  std::vector<std::string> props{"proop", "prooop2"};
  auto res = ScanVertices(client, transaction_id, std::nullopt, props, 3);
  std::cout << "RES:" << res.value() << std::endl;
  res = ScanVertices(client, transaction_id, *res, props, 3);
  std::cout << "RES:" << res.value_or(-1) << std::endl;
  res = ScanVertices(client, transaction_id, std::nullopt, std::nullopt, 3);
  std::cout << "RES:" << res.value() << std::endl;
  res = ScanVertices(client, transaction_id, *res, std::nullopt, 3);
  std::cout << "RES:" << res.value_or(-1) << std::endl;

  return 1;
}
