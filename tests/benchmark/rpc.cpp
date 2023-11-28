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

#include <optional>
#include <thread>
#include <utility>

#include <benchmark/benchmark.h>

#include "rpc/client.hpp"
#include "rpc/client_pool.hpp"
#include "rpc/messages.hpp"
#include "rpc/server.hpp"
#include "slk/serialization.hpp"
#include "utils/timer.hpp"

struct EchoMessage {
  static const memgraph::utils::TypeInfo kType;

  EchoMessage() = default;  // Needed for serialization.
  explicit EchoMessage(std::string data) : data(std::move(data)) {}

  static void Load(EchoMessage *obj, memgraph::slk::Reader *reader);
  static void Save(const EchoMessage &obj, memgraph::slk::Builder *builder);

  std::string data;
};

namespace memgraph::slk {
void Save(const EchoMessage &echo, Builder *builder) { Save(echo.data, builder); }
void Load(EchoMessage *echo, Reader *reader) { Load(&echo->data, reader); }
}  // namespace memgraph::slk

void EchoMessage::Load(EchoMessage *obj, memgraph::slk::Reader *reader) { memgraph::slk::Load(obj, reader); }
void EchoMessage::Save(const EchoMessage &obj, memgraph::slk::Builder *builder) { memgraph::slk::Save(obj, builder); }

const memgraph::utils::TypeInfo EchoMessage::kType{memgraph::utils::TypeId::UNKNOWN, "EchoMessage"};

using Echo = memgraph::rpc::RequestResponse<EchoMessage, EchoMessage>;

const int kThreadsNum = 16;

DEFINE_string(server_address, "127.0.0.1", "Server address");
DEFINE_int32(server_port, 0, "Server port");
DEFINE_string(server_cert_file, "", "Server SSL certificate file");
DEFINE_string(server_key_file, "", "Server SSL key file");
DEFINE_bool(benchmark_use_ssl, false, "Set to true to benchmark using SSL");
DEFINE_bool(run_server, true, "Set to false to use external server");
DEFINE_bool(run_benchmark, true, "Set to false to only run server");

std::optional<memgraph::communication::ServerContext> server_context;
std::optional<memgraph::rpc::Server> server;
std::optional<memgraph::communication::ClientContext> client_context;
std::optional<memgraph::rpc::Client> clients[kThreadsNum];
std::optional<memgraph::rpc::ClientPool> client_pool;

static void BenchmarkRpc(benchmark::State &state) {
  std::string data(state.range(0), 'a');
  while (state.KeepRunning()) {
    clients[state.thread_index()]->Call<Echo>(data);
  }
  state.SetItemsProcessed(state.iterations());
}

static void BenchmarkRpcPool(benchmark::State &state) {
  std::string data(state.range(0), 'a');
  while (state.KeepRunning()) {
    client_pool->Call<Echo>(data);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BenchmarkRpc)
    ->RangeMultiplier(4)
    ->Range(4, 1 << 13)
    ->ThreadRange(1, kThreadsNum)
    ->Unit(benchmark::kNanosecond)
    ->UseRealTime();

BENCHMARK(BenchmarkRpcPool)
    ->RangeMultiplier(4)
    ->Range(4, 1 << 13)
    ->ThreadRange(1, kThreadsNum)
    ->Unit(benchmark::kNanosecond)
    ->UseRealTime();

int main(int argc, char **argv) {
  ::benchmark::Initialize(&argc, argv);
  gflags::AllowCommandLineReparsing();
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_run_server) {
    if (!FLAGS_server_cert_file.empty() && !FLAGS_server_key_file.empty()) {
      FLAGS_benchmark_use_ssl = true;
      server_context.emplace(FLAGS_server_key_file, FLAGS_server_cert_file);
    } else {
      server_context.emplace();
    }
    server.emplace(memgraph::io::network::Endpoint(FLAGS_server_address, FLAGS_server_port), &server_context.value(),
                   kThreadsNum);

    server->Register<Echo>([](const auto &req_reader, auto *res_builder) {
      EchoMessage res;
      Load(&res, req_reader);
      Save(res, res_builder);
    });
    server->Start();
  }

  if (FLAGS_run_benchmark) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    memgraph::io::network ::Endpoint endpoint;
    if (FLAGS_run_server) {
      endpoint = server->endpoint();
    } else {
      endpoint = memgraph::io::network::Endpoint(FLAGS_server_address, FLAGS_server_port);
    }

    client_context.emplace(FLAGS_benchmark_use_ssl);

    for (int i = 0; i < kThreadsNum; ++i) {
      clients[i].emplace(endpoint, &client_context.value());
      clients[i]->Call<Echo>("init");
    }

    // The client pool connects to the server only when there are no leftover
    // unused RPC clients (during concurrent execution). To reduce the overhead
    // of making connections to the server during the benchmark here we
    // simultaneously call the Echo RPC on the client pool to make the client
    // pool connect to the server `kThreadsNum` times.
    client_pool.emplace(endpoint, &client_context.value());
    std::thread threads[kThreadsNum];
    for (int i = 0; i < kThreadsNum; ++i) {
      threads[i] = std::thread([] { client_pool->Call<Echo>(std::string(10000, 'a')); });
    }
    for (int i = 0; i < kThreadsNum; ++i) {
      threads[i].join();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    ::benchmark::RunSpecifiedBenchmarks();
  } else {
    std::this_thread::sleep_for(std::chrono::seconds(3600 * 24 * 365));
  }

  if (FLAGS_run_server) {
    server->Shutdown();
    server->AwaitShutdown();
  }
}
