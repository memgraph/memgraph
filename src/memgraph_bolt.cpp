#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <exception>
#include <functional>
#include <limits>
#include <string>
#include <thread>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/v1/session.hpp"
#include "config.hpp"
#include "database/distributed_graph_db.hpp"
#include "database/graph_db.hpp"
#include "distributed/pull_rpc_clients.hpp"
#include "glue/conversion.hpp"
#include "integrations/kafka/exceptions.hpp"
#include "integrations/kafka/streams.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"
#include "query/transaction_engine.hpp"
#include "requests/requests.hpp"
#include "stats/stats.hpp"
#include "telemetry/telemetry.hpp"
#include "utils/flag_validation.hpp"
#include "utils/signals.hpp"
#include "utils/sysinfo/memory.hpp"
#include "utils/terminate_handler.hpp"
#include "version.hpp"

// Common stuff for enterprise and community editions

// General purpose flags.
DEFINE_string(interface, "0.0.0.0",
              "Communication interface on which to listen.");
DEFINE_VALIDATED_int32(port, 7687, "Communication port on which to listen.",
                       FLAG_IN_RANGE(0, std::numeric_limits<uint16_t>::max()));
DEFINE_VALIDATED_int32(num_workers,
                       std::max(std::thread::hardware_concurrency(), 1U),
                       "Number of workers (Bolt)", FLAG_IN_RANGE(1, INT32_MAX));
DEFINE_VALIDATED_int32(session_inactivity_timeout, 1800,
                       "Time in seconds after which inactive sessions will be "
                       "closed.",
                       FLAG_IN_RANGE(1, INT32_MAX));
DEFINE_string(cert_file, "", "Certificate file to use.");
DEFINE_string(key_file, "", "Key file to use.");
DEFINE_string(log_file, "", "Path to where the log should be stored.");
DEFINE_HIDDEN_string(
    log_link_basename, "",
    "Basename used for symlink creation to the last log file.");
DEFINE_uint64(memory_warning_threshold, 1024,
              "Memory warning threshold, in MB. If Memgraph detects there is "
              "less available RAM it will log a warning. Set to 0 to "
              "disable.");
DEFINE_bool(telemetry_enabled, false,
            "Set to true to enable telemetry. We collect information about the "
            "running system (CPU and memory information) and information about "
            "the database runtime (vertex and edge counts and resource usage) "
            "to allow for easier improvement of the product.");
DECLARE_string(durability_directory);

/** Encapsulates Dbms and Interpreter that are passed through the network server
 * and worker to the session. */
struct SessionData {
  database::GraphDb &db;
  query::Interpreter interpreter{db};
};

class BoltSession final
    : public communication::bolt::Session<communication::InputStream,
                                          communication::OutputStream> {
 public:
  BoltSession(SessionData &data, communication::InputStream &input_stream,
              communication::OutputStream &output_stream)
      : communication::bolt::Session<communication::InputStream,
                                     communication::OutputStream>(
            input_stream, output_stream),
        transaction_engine_(data.db, data.interpreter) {}

  using communication::bolt::Session<communication::InputStream,
                                     communication::OutputStream>::TEncoder;

  std::vector<std::string> Interpret(
      const std::string &query,
      const std::map<std::string, communication::bolt::Value> &params)
      override {
    std::map<std::string, query::TypedValue> params_tv;
    for (const auto &kv : params)
      params_tv.emplace(kv.first, glue::ToTypedValue(kv.second));
    try {
      return transaction_engine_.Interpret(query, params_tv);
    } catch (const query::QueryException &e) {
      // Wrap QueryException into ClientError, because we want to allow the
      // client to fix their query.
      throw communication::bolt::ClientError(e.what());
    }
  }

  std::map<std::string, communication::bolt::Value> PullAll(
      TEncoder *encoder) override {
    try {
      TypedValueResultStream stream(encoder);
      const auto &summary = transaction_engine_.PullAll(&stream);
      std::map<std::string, communication::bolt::Value> decoded_summary;
      for (const auto &kv : summary) {
        decoded_summary.emplace(kv.first, glue::ToBoltValue(kv.second));
      }
      return decoded_summary;
    } catch (const query::QueryException &e) {
      // Wrap QueryException into ClientError, because we want to allow the
      // client to fix their query.
      throw communication::bolt::ClientError(e.what());
    }
  }

  void Abort() override { transaction_engine_.Abort(); }

 private:
  // Wrapper around TEncoder which converts TypedValue to Value
  // before forwarding the calls to original TEncoder.
  class TypedValueResultStream {
   public:
    TypedValueResultStream(TEncoder *encoder) : encoder_(encoder) {}

    void Result(const std::vector<query::TypedValue> &values) {
      std::vector<communication::bolt::Value> decoded_values;
      decoded_values.reserve(values.size());
      for (const auto &v : values) {
        decoded_values.push_back(glue::ToBoltValue(v));
      }
      encoder_->MessageRecord(decoded_values);
    }

   private:
    TEncoder *encoder_;
  };

  query::TransactionEngine transaction_engine_;
};

using ServerT = communication::Server<BoltSession, SessionData>;
using communication::ServerContext;

/**
 * Class that implements ResultStream API for Kafka.
 *
 * Kafka doesn't need to stream the import results back to the client so we
 * don't need any functionality here.
 */
class KafkaResultStream {
 public:
  void Result(const std::vector<query::TypedValue> &) {}
};

// Needed to correctly handle memgraph destruction from a signal handler.
// Without having some sort of a flag, it is possible that a signal is handled
// when we are exiting main, inside destructors of database::GraphDb and
// similar. The signal handler may then initiate another shutdown on memgraph
// which is in half destructed state, causing invalid memory access and crash.
volatile sig_atomic_t is_shutting_down = 0;

/// Set up signal handlers and register `shutdown` on SIGTERM and SIGINT.
/// In most cases you don't have to call this. If you are using a custom server
/// startup function for `WithInit`, then you probably need to use this to
/// shutdown your server.
void InitSignalHandlers(const std::function<void()> &shutdown_fun) {
  // Prevent handling shutdown inside a shutdown. For example, SIGINT handler
  // being interrupted by SIGTERM before is_shutting_down is set, thus causing
  // double shutdown.
  sigset_t block_shutdown_signals;
  sigemptyset(&block_shutdown_signals);
  sigaddset(&block_shutdown_signals, SIGTERM);
  sigaddset(&block_shutdown_signals, SIGINT);

  // Wrap the shutdown function in a safe way to prevent recursive shutdown.
  auto shutdown = [shutdown_fun]() {
    if (is_shutting_down) return;
    is_shutting_down = 1;
    shutdown_fun();
  };

  CHECK(utils::SignalHandler::RegisterHandler(utils::Signal::Terminate,
                                              shutdown, block_shutdown_signals))
      << "Unable to register SIGTERM handler!";
  CHECK(utils::SignalHandler::RegisterHandler(utils::Signal::Interupt, shutdown,
                                              block_shutdown_signals))
      << "Unable to register SIGINT handler!";

  // Setup SIGUSR1 to be used for reopening log files, when e.g. logrotate
  // rotates our logs.
  CHECK(utils::SignalHandler::RegisterHandler(utils::Signal::User1, []() {
    google::CloseLogDestination(google::INFO);
  })) << "Unable to register SIGUSR1 handler!";
}

/// Run the Memgraph server.
///
/// Sets up all the required state before running `memgraph_main` and does any
/// required cleanup afterwards.  `get_stats_prefix` is used to obtain the
/// prefix when logging Memgraph's statistics.
///
/// Command line arguments and configuration files are read before calling any
/// of the supplied functions. Therefore, you should use flags only from those
/// functions, and *not before* invoking `WithInit`.
///
/// This should be the first and last thing a OS specific main function does.
///
/// A common example of usage is:
///
/// @code
/// int main(int argc, char *argv[]) {
///   auto get_stats_prefix = []() -> std::string { return "memgraph"; };
///   return WithInit(argc, argv, get_stats_prefix, SingleNodeMain);
/// }
/// @endcode
///
/// If you wish to start Memgraph server in another way, you can pass a
/// `memgraph_main` functions which does that. You should take care to call
/// `InitSignalHandlers` with appropriate function to shutdown the server you
/// started.
int WithInit(int argc, char **argv,
             const std::function<std::string()> &get_stats_prefix,
             const std::function<void()> &memgraph_main) {
  gflags::SetVersionString(version_string);

  // Load config before parsing arguments, so that flags from the command line
  // overwrite the config.
  LoadConfig();
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  google::InitGoogleLogging(argv[0]);
  google::SetLogDestination(google::INFO, FLAGS_log_file.c_str());
  google::SetLogSymlink(google::INFO, FLAGS_log_link_basename.c_str());

  // Unhandled exception handler init.
  std::set_terminate(&utils::TerminateHandler);

  stats::InitStatsLogging(get_stats_prefix());
  utils::OnScopeExit stop_stats([] { stats::StopStatsLogging(); });

  // Initialize the communication library.
  communication::Init();

  // Start memory warning logger.
  utils::Scheduler mem_log_scheduler;
  if (FLAGS_memory_warning_threshold > 0) {
    mem_log_scheduler.Run("Memory warning", std::chrono::seconds(3), [] {
      auto free_ram_mb = utils::sysinfo::AvailableMem() / 1024;
      if (free_ram_mb < FLAGS_memory_warning_threshold)
        LOG(WARNING) << "Running out of available RAM, only " << free_ram_mb
                     << " MB left.";
    });
  }
  requests::Init();

  memgraph_main();
  return 0;
}

void SingleNodeMain() {
  google::SetUsageMessage("Memgraph single-node database server");
  database::SingleNode db;
  SessionData session_data{db};

  auto stream_writer =
      [&session_data](const std::vector<std::string> &queries) {
        for (auto &query : queries) {
          database::GraphDbAccessor dba(session_data.db);
          KafkaResultStream stream;
          try {
            session_data.interpreter(query, dba, {}, false).PullAll(stream);
            dba.Commit();
          } catch (const query::QueryException &e) {
            LOG(ERROR) << e.what();
            dba.Abort();
          }
        }
      };

  integrations::kafka::Streams kafka_streams{
      std::experimental::filesystem::path(FLAGS_durability_directory) /
          "streams",
      stream_writer};

  try {
    // Recover possible streams.
    kafka_streams.Recover();
  } catch (const integrations::kafka::KafkaStreamException &e) {
    LOG(ERROR) << e.what();
  }

  session_data.interpreter.kafka_streams_ = &kafka_streams;

  ServerContext context;
  std::string service_name = "Bolt";
  if (FLAGS_key_file != "" && FLAGS_cert_file != "") {
    context = ServerContext(FLAGS_key_file, FLAGS_cert_file);
    service_name = "BoltS";
  }

  ServerT server({FLAGS_interface, static_cast<uint16_t>(FLAGS_port)},
                 session_data, &context, FLAGS_session_inactivity_timeout,
                 service_name, FLAGS_num_workers);

  // Setup telemetry
  std::experimental::optional<telemetry::Telemetry> telemetry;
  if (FLAGS_telemetry_enabled) {
    telemetry.emplace(
        "https://telemetry.memgraph.com/88b5e7e8-746a-11e8-9f85-538a9e9690cc/",
        std::experimental::filesystem::path(FLAGS_durability_directory) /
            "telemetry",
        std::chrono::minutes(10));
    telemetry->AddCollector("db", [&db]() -> nlohmann::json {
      database::GraphDbAccessor dba(db);
      return {{"vertices", dba.VerticesCount()}, {"edges", dba.EdgesCount()}};
    });
  }

  // Handler for regular termination signals
  auto shutdown = [&server] {
    // Server needs to be shutdown first and then the database. This prevents a
    // race condition when a transaction is accepted during server shutdown.
    server.Shutdown();
  };
  InitSignalHandlers(shutdown);

  server.AwaitShutdown();
}

// End common stuff for enterprise and community editions

#ifdef MG_COMMUNITY

int main(int argc, char **argv) {
  return WithInit(argc, argv, []() { return "memgraph"; }, SingleNodeMain);
}

#else  // enterprise edition

// Distributed flags.
DEFINE_HIDDEN_bool(
    master, false,
    "If this Memgraph server is the master in a distributed deployment.");
DEFINE_HIDDEN_bool(
    worker, false,
    "If this Memgraph server is a worker in a distributed deployment.");
DECLARE_int32(worker_id);

void MasterMain() {
  google::SetUsageMessage("Memgraph distributed master");

  database::Master db;
  SessionData session_data{db};

  auto stream_writer =
      [&session_data](const std::vector<std::string> &queries) {
        for (auto &query : queries) {
          database::GraphDbAccessor dba(session_data.db);
          KafkaResultStream stream;
          try {
            session_data.interpreter(query, dba, {}, false).PullAll(stream);
            dba.Commit();
          } catch (const query::QueryException &e) {
            LOG(ERROR) << e.what();
            dba.Abort();
          }
        }
      };

  integrations::kafka::Streams kafka_streams{
      std::experimental::filesystem::path(FLAGS_durability_directory) /
          "streams",
      stream_writer};

  try {
    // Recover possible streams.
    kafka_streams.Recover();
  } catch (const integrations::kafka::KafkaStreamException &e) {
    LOG(ERROR) << e.what();
  }

  session_data.interpreter.kafka_streams_ = &kafka_streams;

  ServerContext context;
  std::string service_name = "Bolt";
  if (FLAGS_key_file != "" && FLAGS_cert_file != "") {
    context = ServerContext(FLAGS_key_file, FLAGS_cert_file);
    service_name = "BoltS";
  }

  ServerT server({FLAGS_interface, static_cast<uint16_t>(FLAGS_port)},
                 session_data, &context, FLAGS_session_inactivity_timeout,
                 service_name, FLAGS_num_workers);

  // Handler for regular termination signals
  auto shutdown = [&server] {
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

int main(int argc, char **argv) {
  auto get_stats_prefix = [&]() -> std::string {
    if (FLAGS_master) {
      return "master";
    } else if (FLAGS_worker) {
      return fmt::format("worker-{}", FLAGS_worker_id);
    }
    return "memgraph";
  };

  auto memgraph_main = [&]() {
    CHECK(!(FLAGS_master && FLAGS_worker))
        << "Can't run Memgraph as worker and master at the same time";
    if (FLAGS_master)
      MasterMain();
    else if (FLAGS_worker)
      WorkerMain();
    else
      SingleNodeMain();
  };

  return WithInit(argc, argv, get_stats_prefix, memgraph_main);
}

#endif  // enterprise edition
