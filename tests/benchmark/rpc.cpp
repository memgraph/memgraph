#include <optional>
#include <thread>

#include <benchmark/benchmark.h>

#include "communication/rpc/client.hpp"
#include "communication/rpc/client_pool.hpp"
#include "communication/rpc/messages.hpp"
#include "communication/rpc/server.hpp"
#include "slk/serialization.hpp"
#include "utils/timer.hpp"

struct EchoMessage {
  static const utils::TypeInfo kType;

  EchoMessage() {}  // Needed for serialization.
  EchoMessage(const std::string &data) : data(data) {}

  static void Load(EchoMessage *obj, slk::Reader *reader);
  static void Save(const EchoMessage &obj, slk::Builder *builder);

  std::string data;
};

namespace slk {
void Save(const EchoMessage &echo, Builder *builder) {
  Save(echo.data, builder);
}
void Load(EchoMessage *echo, Reader *reader) { Load(&echo->data, reader); }
}  // namespace slk

void EchoMessage::Load(EchoMessage *obj, slk::Reader *reader) {
  slk::Load(obj, reader);
}
void EchoMessage::Save(const EchoMessage &obj, slk::Builder *builder) {
  slk::Save(obj, builder);
}

const utils::TypeInfo EchoMessage::kType{2, "EchoMessage"};

using Echo = communication::rpc::RequestResponse<EchoMessage, EchoMessage>;

const int kThreadsNum = 16;

DEFINE_string(server_address, "127.0.0.1", "Server address");
DEFINE_int32(server_port, 0, "Server port");
DEFINE_bool(run_server, true, "Set to false to use external server");
DEFINE_bool(run_benchmark, true, "Set to false to only run server");

std::optional<communication::rpc::Server> server;
std::optional<communication::rpc::Client> clients[kThreadsNum];
std::optional<communication::rpc::ClientPool> client_pool;
std::optional<utils::ThreadPool> thread_pool;

static void BenchmarkRpc(benchmark::State &state) {
  std::string data(state.range(0), 'a');
  while (state.KeepRunning()) {
    clients[state.thread_index]->Call<Echo>(data);
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

static void BenchmarkRpcPoolAsync(benchmark::State &state) {
  std::string data(state.range(0), 'a');
  while (state.KeepRunning()) {
    auto future = thread_pool->Run([&data] { client_pool->Call<Echo>(data); });
    future.get();
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

BENCHMARK(BenchmarkRpcPoolAsync)
    ->RangeMultiplier(4)
    ->Range(4, 1 << 13)
    ->ThreadRange(1, kThreadsNum)
    ->Unit(benchmark::kNanosecond)
    ->UseRealTime();

int main(int argc, char **argv) {
  ::benchmark::Initialize(&argc, argv);
  gflags::AllowCommandLineReparsing();
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  if (FLAGS_run_server) {
    server.emplace(
        io::network::Endpoint(FLAGS_server_address, FLAGS_server_port),
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

    io::network ::Endpoint endpoint;
    if (FLAGS_run_server) {
      endpoint = server->endpoint();
    } else {
      endpoint = io::network::Endpoint(FLAGS_server_address, FLAGS_server_port);
    }

    for (int i = 0; i < kThreadsNum; ++i) {
      clients[i].emplace(endpoint);
      clients[i]->Call<Echo>("init");
    }

    // The client pool connects to the server only when there are no leftover
    // unused RPC clients (during concurrent execution). To reduce the overhead
    // of making connections to the server during the benchmark here we
    // simultaneously call the Echo RPC on the client pool to make the client
    // pool connect to the server `kThreadsNum` times.
    client_pool.emplace(endpoint);
    std::thread threads[kThreadsNum];
    for (int i = 0; i < kThreadsNum; ++i) {
      threads[i] =
          std::thread([] { client_pool->Call<Echo>(std::string(10000, 'a')); });
    }
    for (int i = 0; i < kThreadsNum; ++i) {
      threads[i].join();
    }

    thread_pool.emplace(kThreadsNum, "RPC client");

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
