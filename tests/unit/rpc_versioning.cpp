// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <atomic>

#include "rpc_messages.hpp"

#include "rpc/client.hpp"
#include "rpc/server.hpp"
#include "rpc/utils.hpp"  // Needs to be included last so that SLK definitions are seen

using memgraph::communication::ClientContext;
using memgraph::communication::ServerContext;

using memgraph::io::network::Endpoint;
using memgraph::rpc::Client;
using memgraph::rpc::GenericRpcFailedException;
using memgraph::rpc::Server;
using memgraph::slk::Load;
using memgraph::storage::replication::HeartbeatRpc;

using namespace std::string_view_literals;
using namespace std::literals::chrono_literals;

namespace memgraph::slk {
void Save(const SumReq &sum, Builder *builder) {
  Save(sum.x, builder);
  Save(sum.y, builder);
}

void Load(SumReq *sum, Reader *reader) {
  Load(&sum->x, reader);
  Load(&sum->y, reader);
}

void Save(const SumReqV2 &sum, Builder *builder) { Save(sum.nums_, builder); }

void Load(SumReqV2 *sum, Reader *reader) { Load(&sum->nums_, reader); }

void Save(const SumRes &res, Builder *builder) { Save(res.sum, builder); }
void Load(SumRes *res, Reader *reader) { Load(&res->sum, reader); }
}  // namespace memgraph::slk

void SumReq::Load(SumReq *obj, memgraph::slk::Reader *reader) { memgraph::slk::Load(obj, reader); }
void SumReq::Save(const SumReq &obj, memgraph::slk::Builder *builder) { memgraph::slk::Save(obj, builder); }

void SumReqV2::Load(SumReqV2 *obj, memgraph::slk::Reader *reader) { memgraph::slk::Load(obj, reader); }
void SumReqV2::Save(const SumReqV2 &obj, memgraph::slk::Builder *builder) { memgraph::slk::Save(obj, builder); }

void SumRes::Load(SumRes *obj, memgraph::slk::Reader *reader) { memgraph::slk::Load(obj, reader); }
void SumRes::Save(const SumRes &obj, memgraph::slk::Builder *builder) { memgraph::slk::Save(obj, builder); }

namespace {
constexpr int port{8182};

template <typename Target, typename Old>
Target UpgradeToTarget(Old const &old) {
  if constexpr (std::is_same_v<Target, std::decay_t<Old>>) {
    return old;
  } else {
    auto const intermediate = old.Upgrade();
    using IntermediateType = decltype(intermediate);

    static_assert(IntermediateType::kVersion == Old::kVersion + 1, "Upgrade needs to lead to the new type");

    return UpgradeToTarget<Target>(intermediate);
  }
}

template <typename V>
using last_variant_type = std::variant_alternative_t<std::variant_size_v<V> - 1, V>;

// You can use lambdas and write factory with the help of map instead of if-else
template <typename V>
auto GetLatestSumReqType(V const &types) -> last_variant_type<V> {
  using TargetType = last_variant_type<V>;
  return std::visit([](auto const &current_type) -> TargetType { return UpgradeToTarget<TargetType>(current_type); },
                    types);
}

}  // namespace

// RPC client is setup with timeout but shouldn't be triggered.
TEST(RpcVersioning, RequestUpgrade) {
  Endpoint const endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    rpc_server.Shutdown();
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Sum>([](uint64_t const request_version, auto *req_reader, auto *res_builder) {
    // Option I:
    // auto const var = SumReqFactory(request_version, req_reader);

    // Option II: Map factory
    auto const var = std::invoke(sum_req_factory.at(request_version), req_reader);
    auto const req = GetLatestSumReqType(var);
    /*
    auto const req = std::invoke([&request_version, req_reader]()-> SumReqV2 {
      if (request_version == SumReqV2::kVersion) {
        auto local_req = SumReqV2{};
        Load(&local_req, req_reader);
        return local_req;
      }
      if (request_version == SumReq::kVersion) {
        auto local_req = SumReq{};
        Load(&local_req, req_reader);
        return local_req.Upgrade();
      }
      LOG_FATAL("Unknown version request");
    });
    */

    auto const sum = std::accumulate(req.nums_.begin(), req.nums_.end(), 0);

    SumRes const res(sum);
    memgraph::rpc::SendFinalResponse(res, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("EchoMessage"sv, 2000)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};
  {
    auto stream = client.Stream<Sum>(10, 12);
    auto reply = stream.SendAndWait();
    EXPECT_EQ(reply.sum, 22);
  }
  {
    auto stream = client.Stream<SumV2>(std::initializer_list<int>{35, 30});
    auto reply = stream.SendAndWait();
    EXPECT_EQ(reply.sum, 65);
  }
}
