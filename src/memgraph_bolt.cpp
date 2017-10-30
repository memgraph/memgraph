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
DEFINE_string(log_link_basename, "",
              "Basename used for symlink creation to the last log file.");
DEFINE_uint64(memory_warning_threshold, 1024,
              "Memory warning treshold, in MB. If Memgraph detects there is "
              "less available RAM available it will log a warning. Set to 0 to "
              "disable.");

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

  // Signal handling init.
  SignalHandler::register_handler(Signal::SegmentationFault, []() {
    // Log that we got SIGSEGV and abort the program, because returning from
    // SIGSEGV handler is undefined behaviour.
    std::cerr << "SegmentationFault signal raised" << std::endl;
    std::abort();  // This will continue into our SIGABRT handler.
  });
  SignalHandler::register_handler(Signal::Abort, []() {
    // Log the stacktrace and let the abort continue.
    Stacktrace stacktrace;
    std::cerr << "Abort signal raised" << std::endl
              << stacktrace.dump() << std::endl;
  });

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

  auto shutdown = [&server, &session_data]() {
    // Server needs to be shutdown first and then the database. This prevents a
    // race condition when a transaction is accepted during server shutdown.
    server.Shutdown();
    session_data.db.Shutdown();
  };
  // register SIGTERM handler
  SignalHandler::register_handler(Signal::Terminate, shutdown);

  // register SIGINT handler
  SignalHandler::register_handler(Signal::Interupt, shutdown);

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
