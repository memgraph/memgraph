#include <csignal>
#include <experimental/filesystem>
#include <iostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/v1/session.hpp"
#include "communication/server.hpp"
#include "config.hpp"
#include "io/network/network_endpoint.hpp"
#include "io/network/network_error.hpp"
#include "io/network/socket.hpp"
#include "utils/flag_validation.hpp"
#include "utils/scheduler.hpp"
#include "utils/signals/handler.hpp"
#include "utils/stacktrace.hpp"
#include "utils/sysinfo/memory.hpp"
#include "utils/terminate_handler.hpp"
#include "version.hpp"

namespace fs = std::experimental::filesystem;
using communication::bolt::SessionData;
using io::network::NetworkEndpoint;
using io::network::Socket;
using SessionT = communication::bolt::Session<Socket>;
using ResultStreamT = SessionT::ResultStreamT;
using ServerT = communication::Server<SessionT, SessionData>;

DEFINE_string(interface, "0.0.0.0",
              "Communication interface on which to listen.");
DEFINE_string(port, "7687", "Communication port on which to listen.");
DEFINE_VALIDATED_int32(num_workers,
                       std::max(std::thread::hardware_concurrency(), 1U),
                       "Number of workers", FLAG_IN_RANGE(1, INT32_MAX));
DEFINE_string(log_file, "", "Path to where the log should be stored.");
DEFINE_HIDDEN_string(
    log_link_basename, "",
    "Basename used for symlink creation to the last log file.");
DEFINE_uint64(memory_warning_threshold, 1024,
              "Memory warning treshold, in MB. If Memgraph detects there is "
              "less available RAM available it will log a warning. Set to 0 to "
              "disable.");

// Needed to correctly handle memgraph destruction from a signal handler.
// Without having some sort of a flag, it is possible that a signal is handled
// when we are exiting main, inside destructors of GraphDb and similar. The
// signal handler may then initiate another shutdown on memgraph which is in
// half destructed state, causing invalid memory access and crash.
volatile sig_atomic_t is_shutting_down = 0;

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph database server");
  gflags::SetVersionString(version_string);

  // Load config before parsing arguments, so that flags from the command line
  // overwrite the config.
  LoadConfig();
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  google::InitGoogleLogging(argv[0]);
  google::SetLogDestination(google::INFO, FLAGS_log_file.c_str());
  google::SetLogSymlink(google::INFO, FLAGS_log_link_basename.c_str());

  // Unhandled exception handler init.
  std::set_terminate(&terminate_handler);

  // Initialize bolt session data (GraphDb and Interpreter).
  SessionData session_data;

  // Initialize endpoint.
  NetworkEndpoint endpoint = [&] {
    try {
      return NetworkEndpoint(FLAGS_interface, FLAGS_port);
    } catch (io::network::NetworkEndpointException &e) {
      LOG(FATAL) << e.what();
    }
  }();

  // Initialize server.
  ServerT server(endpoint, session_data);

  // Handler for regular termination signals
  auto shutdown = [&server, &session_data]() {
    if (is_shutting_down) return;
    is_shutting_down = 1;
    // Server needs to be shutdown first and then the database. This prevents a
    // race condition when a transaction is accepted during server shutdown.
    server.Shutdown();
    session_data.db.Shutdown();
  };

  // Prevent handling shutdown inside a shutdown. For example, SIGINT handler
  // being interrupted by SIGTERM before is_shutting_down is set, thus causing
  // double shutdown.
  sigset_t block_shutdown_signals;
  sigemptyset(&block_shutdown_signals);
  sigaddset(&block_shutdown_signals, SIGTERM);
  sigaddset(&block_shutdown_signals, SIGINT);

  CHECK(SignalHandler::RegisterHandler(Signal::Terminate, shutdown,
                                       block_shutdown_signals))
      << "Unable to register SIGTERM handler!";
  CHECK(SignalHandler::RegisterHandler(Signal::Interupt, shutdown,
                                       block_shutdown_signals))
      << "Unable to register SIGINT handler!";

  // Setup SIGUSR1 to be used for reopening log files, when e.g. logrotate
  // rotates our logs.
  CHECK(SignalHandler::RegisterHandler(Signal::User1, []() {
    google::CloseLogDestination(google::INFO);
  })) << "Unable to register SIGUSR1 handler!";

  // Start memory warning logger.
  Scheduler mem_log_scheduler;
  if (FLAGS_memory_warning_threshold > 0) {
    mem_log_scheduler.Run(std::chrono::seconds(3), [] {
      auto free_ram_mb = utils::AvailableMem() / 1024;
      if (free_ram_mb < FLAGS_memory_warning_threshold)
        LOG(WARNING) << "Running out of available RAM, only " << free_ram_mb
                     << " MB left.";
    });
  }

  // Start worker threads.
  server.Start(FLAGS_num_workers);

  return 0;
}
