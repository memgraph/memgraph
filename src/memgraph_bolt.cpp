#include <experimental/filesystem>
#include <iostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/v1/session.hpp"
#include "communication/server.hpp"

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
using io::network::NetworkEndpoint;
using io::network::Socket;
using communication::bolt::SessionData;
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

// Load flags in this order, the last one has the highest priority:
// 1) /etc/memgraph/memgraph.conf
// 2) ~/.memgraph/config
// 3) env - MEMGRAPH_CONFIG
// 4) command line flags

void LoadConfig(int &argc, char **&argv) {
  std::vector<fs::path> configs = {fs::path("/etc/memgraph/memgraph.conf")};
  if (getenv("HOME") != nullptr)
    configs.emplace_back(fs::path(getenv("HOME")) /
                         fs::path(".memgraph/config"));
  {
    auto memgraph_config = getenv("MEMGRAPH_CONFIG");
    if (memgraph_config != nullptr) {
      auto path = fs::path(memgraph_config);
      CHECK(fs::exists(path))
          << "MEMGRAPH_CONFIG environment variable set to nonexisting path: "
          << path.generic_string();
      configs.emplace_back(path);
    }
  }

  std::vector<std::string> flagfile_arguments;
  for (const auto &config : configs)
    if (fs::exists(config)) {
      flagfile_arguments.emplace_back(
          std::string("--flag-file=" + config.generic_string()));
    }

  int custom_argc = static_cast<int>(flagfile_arguments.size()) + 1;
  char **custom_argv = new char *[custom_argc];

  custom_argv[0] = strdup(std::string("memgraph").c_str());
  for (int i = 0; i < static_cast<int>(flagfile_arguments.size()); ++i) {
    custom_argv[i + 1] = strdup(flagfile_arguments[i].c_str());
  }

  // setup flags from config flags
  gflags::ParseCommandLineFlags(&custom_argc, &custom_argv, false);

  // unconsumed arguments have to be freed to avoid memory leak since they are
  // strdup-ed.
  for (int i = 0; i < custom_argc; ++i) free(custom_argv[i]);
  delete[] custom_argv;

  // setup flags from command line
  gflags::ParseCommandLineFlags(&argc, &argv, true);
}

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph database server");
  gflags::SetVersionString(version_string);

  LoadConfig(argc, argv);

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

  // Initialize bolt session data (Dbms and Interpreter).
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

  // register SIGTERM handler
  SignalHandler::register_handler(Signal::Terminate,
                                  [&server, &session_data]() {
                                    server.Shutdown();
                                    session_data.dbms.Shutdown();
                                  });

  // register SIGINT handler
  SignalHandler::register_handler(Signal::Interupt, [&server, &session_data]() {
    server.Shutdown();
    session_data.dbms.Shutdown();
  });

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
