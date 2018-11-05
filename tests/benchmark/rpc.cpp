#include <experimental/optional>
#include <thread>

#include <benchmark/benchmark.h>

#include "capnp/serialize.h"

#include "communication/rpc/client.hpp"
#include "communication/rpc/client_pool.hpp"
#include "communication/rpc/messages.hpp"
#include "communication/rpc/server.hpp"
#include "utils/timer.hpp"

struct EchoMessage {
  using Capnp = ::capnp::AnyPointer;
  static const communication::rpc::MessageType TypeInfo;

  EchoMessage() {}  // Needed for serialization.
  EchoMessage(const std::string &data) : data(data) {}

  std::string data;
};

void Save(const EchoMessage &echo, ::capnp::AnyPointer::Builder *builder) {
  auto list_builder = builder->initAs<::capnp::List<::capnp::Text>>(1);
  list_builder.set(0, echo.data);
}

void Load(EchoMessage *echo, const ::capnp::AnyPointer::Reader &reader) {
  auto list_reader = reader.getAs<::capnp::List<::capnp::Text>>();
  echo->data = list_reader[0];
}

const communication::rpc::MessageType EchoMessage::TypeInfo{2, "EchoMessage"};

using Echo = communication::rpc::RequestResponse<EchoMessage, EchoMessage>;

const int kThreadsNum = 16;

DEFINE_string(server_address, "127.0.0.1", "Server address");
DEFINE_int32(server_port, 0, "Server port");
DEFINE_bool(run_server, true, "Set to false to use external server");
DEFINE_bool(run_benchmark, true, "Set to false to only run server");

std::experimental::optional<communication::rpc::Server> server;
std::experimental::optional<communication::rpc::Client> clients[kThreadsNum];

static void BenchmarkRpc(benchmark::State &state) {
  std::string data('a', state.range(0));
  while (state.KeepRunning()) {
    clients[state.thread_index]->Call<Echo>(data);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BenchmarkRpc)
    ->RangeMultiplier(4)
    ->Range(4, 1 << 13)
    ->ThreadRange(1, kThreadsNum)
    ->Unit(benchmark::kNanosecond)
    ->UseRealTime();

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  ::benchmark::Initialize(&argc, argv);

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

    for (int i = 0; i < kThreadsNum; ++i) {
      if (FLAGS_run_server) {
        clients[i].emplace(server->endpoint());
      } else {
        clients[i].emplace(
            io::network::Endpoint(FLAGS_server_address, FLAGS_server_port));
      }
      clients[i]->Call<Echo>("init");
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
