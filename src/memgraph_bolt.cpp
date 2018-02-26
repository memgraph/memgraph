#include <csignal>
#include <experimental/filesystem>
#include <iostream>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/v1/session.hpp"
#include "communication/server.hpp"
#include "config.hpp"
#include "database/graph_db.hpp"
#include "distributed/coordination_master.hpp"
#include "distributed/coordination_worker.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/network_error.hpp"
#include "io/network/socket.hpp"
#include "stats/stats.hpp"
#include "utils/flag_validation.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/scheduler.hpp"
#include "utils/signals/handler.hpp"
#include "utils/stacktrace.hpp"
#include "utils/sysinfo/memory.hpp"
#include "utils/terminate_handler.hpp"
#include "version.hpp"

namespace fs = std::experimental::filesystem;
using communication::bolt::SessionData;
using io::network::Endpoint;
using io::network::Socket;
using SessionT = communication::bolt::Session<Socket>;
using ServerT = communication::Server<SessionT, SessionData>;

// General purpose flags.
DEFINE_string(interface, "0.0.0.0",
              "Communication interface on which to listen.");
DEFINE_VALIDATED_int32(port, 7687, "Communication port on which to listen.",
                       FLAG_IN_RANGE(0, std::numeric_limits<uint16_t>::max()));
DEFINE_VALIDATED_int32(num_workers,
                       std::max(std::thread::hardware_concurrency(), 1U),
                       "Number of workers (Bolt)", FLAG_IN_RANGE(1, INT32_MAX));
DEFINE_string(log_file, "", "Path to where the log should be stored.");
DEFINE_HIDDEN_string(
    log_link_basename, "",
    "Basename used for symlink creation to the last log file.");
DEFINE_uint64(memory_warning_threshold, 1024,
              "Memory warning threshold, in MB. If Memgraph detects there is "
              "less available RAM it will log a warning. Set to 0 to "
              "disable.");

// Distributed flags.
DEFINE_HIDDEN_bool(
    master, false,
    "If this Memgraph server is the master in a distributed deployment.");
DEFINE_HIDDEN_bool(
    worker, false,
    "If this Memgraph server is a worker in a distributed deployment.");
DECLARE_int32(worker_id);

// Needed to correctly handle memgraph destruction from a signal handler.
// Without having some sort of a flag, it is possible that a signal is handled
// when we are exiting main, inside destructors of database::GraphDb and
// similar. The signal handler may then initiate another shutdown on memgraph
// which is in half destructed state, causing invalid memory access and crash.
volatile sig_atomic_t is_shutting_down = 0;

// Registers the given shutdown function with the appropriate signal handlers.
// See implementation for details.
void InitSignalHandlers(const std::function<void()> &shutdown) {
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
}

void MasterMain() {
  google::SetUsageMessage("Memgraph distributed master");

  database::Master db;
  SessionData session_data{db};
  ServerT server({FLAGS_interface, static_cast<uint16_t>(FLAGS_port)},
                 session_data, true, "Bolt", FLAGS_num_workers);

  // Handler for regular termination signals
  auto shutdown = [&server] {
    if (is_shutting_down) return;
    is_shutting_down = 1;
    // Server needs to be shutdown first and then the database. This prevents a
    // race condition when a transaction is accepted during server shutdown.
    server.Shutdown();
  };

  InitSignalHandlers(shutdown);
  server.AwaitShutdown();
}

void WorkerMain() {
  google::SetUsageMessage("Memgraph distributed worker");
  database::Worker db;
  db.WaitForShutdown();
}

void SingleNodeMain() {
  google::SetUsageMessage("Memgraph single-node database server");
  database::SingleNode db;
  SessionData session_data{db};
  ServerT server({FLAGS_interface, static_cast<uint16_t>(FLAGS_port)},
                 session_data, true, "Bolt", FLAGS_num_workers);

  // Handler for regular termination signals
  auto shutdown = [&server] {
    if (is_shutting_down) return;
    is_shutting_down = 1;
    // Server needs to be shutdown first and then the database. This prevents a
    // race condition when a transaction is accepted during server shutdown.
    server.Shutdown();
  };
  InitSignalHandlers(shutdown);

  server.AwaitShutdown();
}

int main(int argc, char **argv) {
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

  std::string stats_prefix;
  if (FLAGS_master) {
    stats_prefix = "master";
  } else if (FLAGS_worker) {
    stats_prefix = fmt::format("worker-{}", FLAGS_worker_id);
  } else {
    stats_prefix = "memgraph";
  }
  stats::InitStatsLogging(stats_prefix);
  utils::OnScopeExit stop_stats([] { stats::StopStatsLogging(); });

  // Start memory warning logger.
  Scheduler mem_log_scheduler;
  if (FLAGS_memory_warning_threshold > 0) {
    mem_log_scheduler.Run("Memory warning", std::chrono::seconds(3), [] {
      auto free_ram_mb = utils::AvailableMem() / 1024;
      if (free_ram_mb < FLAGS_memory_warning_threshold)
        LOG(WARNING) << "Running out of available RAM, only " << free_ram_mb
                     << " MB left.";
    });
  }

  CHECK(!(FLAGS_master && FLAGS_worker))
      << "Can't run Memgraph as worker and master at the same time";
  if (FLAGS_master)
    MasterMain();
  else if (FLAGS_worker)
    WorkerMain();
  else
    SingleNodeMain();
  return 0;
}
