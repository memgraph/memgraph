#include <ctime>
#include <random>
#include <thread>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/rpc/client.hpp"
#include "io/network/endpoint.hpp"
#include "messages.hpp"
#include "utils/network.hpp"
#include "utils/signals/handler.hpp"
#include "utils/terminate_handler.hpp"

using namespace communication::rpc;
using namespace std::literals::chrono_literals;

DEFINE_string(server_interface, "127.0.0.1",
              "Server interface on which to communicate.");
DEFINE_int32(server_port, 8010, "Server port on which to communicate.");

volatile sig_atomic_t is_shutting_down = 0;

int main(int argc, char **argv) {
  google::SetUsageMessage("Raft RPC Client");

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  // Initialize client.
  Client client(
      io::network::Endpoint(utils::ResolveHostname(FLAGS_server_interface),
                            FLAGS_server_port),
      "main");

  // Try to send 100 values to server.
  // If requests timeout, try to resend it.
  // Log output on server should contain all values once
  // in correct order.
  for (int i = 1; i <= 100; ++i) {
    LOG(INFO) << fmt::format("Apennding value: {}", i);
    auto result_tuple = client.Call<AppendEntry>(i);
    if (!result_tuple) {
      LOG(INFO) << "Request unsuccessful";
      // Try to resend value
      --i;
    } else {
      LOG(INFO) << fmt::format("Appended value: {}", i);
    }
  }

  return 0;
}
