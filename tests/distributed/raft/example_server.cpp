#include <fstream>
#include <thread>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/rpc/server.hpp"
#include "messages.hpp"
#include "utils/signals.hpp"
#include "utils/terminate_handler.hpp"

using namespace communication::rpc;
using namespace std::literals::chrono_literals;

DEFINE_string(interface, "127.0.0.1",
              "Communication interface on which to listen.");
DEFINE_string(port, "10000", "Communication port on which to listen.");
DEFINE_string(log, "log.txt", "Entries log file");

volatile sig_atomic_t is_shutting_down = 0;

int main(int argc, char **argv) {
  google::SetUsageMessage("Raft RPC Server");

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  // Unhandled exception handler init.
  std::set_terminate(&utils::TerminateHandler);

  Server server(io::network::Endpoint(FLAGS_interface, stoul(FLAGS_port)));
  std::ofstream log(FLAGS_log, std::ios_base::app);

  // Handler for regular termination signals.
  auto shutdown = [&log]() {
    if (is_shutting_down) return;
    is_shutting_down = 1;
    log.close();
    exit(0);
  };

  // Prevent handling shutdown inside a shutdown. For example, SIGINT handler
  // being interrupted by SIGTERM before is_shutting_down is set, thus causing
  // double shutdown.
  sigset_t block_shutdown_signals;
  sigemptyset(&block_shutdown_signals);
  sigaddset(&block_shutdown_signals, SIGTERM);
  sigaddset(&block_shutdown_signals, SIGINT);

  CHECK(utils::SignalHandler::RegisterHandler(utils::Signal::Terminate,
                                              shutdown, block_shutdown_signals))
      << "Unable to register SIGTERM handler!";
  CHECK(utils::SignalHandler::RegisterHandler(utils::Signal::Interupt, shutdown,
                                              block_shutdown_signals))
      << "Unable to register SIGINT handler!";

  // Example callback.
  // TODO: Serialize RPC via Cap'n Proto
  // server.Register<AppendEntry>(
  //     [&log](const auto &req_reader, auto *res_builder) {
  //       AppendEntryReq request;
  //       request.Load(req_reader);
  //       log << request.val << std::endl;
  //       log.flush();
  //       LOG(INFO) << fmt::format("AppendEntry: {}", request.val);
  //       AppendEntryRes res(200, FLAGS_interface, stol(FLAGS_port));
  //       res.Save(res_builder);
  //     });

  LOG(INFO) << "Raft RPC server started";
  // Sleep until shutdown detected.
  std::this_thread::sleep_until(
      std::chrono::time_point<std::chrono::system_clock>::max());

  return 0;
}
