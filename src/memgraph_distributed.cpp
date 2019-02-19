#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <limits>
#include <thread>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/server.hpp"
#include "database/distributed/distributed_graph_db.hpp"
#include "integrations/kafka/exceptions.hpp"
#include "integrations/kafka/streams.hpp"
#include "memgraph_init.hpp"
#include "query/distributed_interpreter.hpp"
#include "query/exceptions.hpp"
#include "telemetry/telemetry.hpp"
#include "utils/flag_validation.hpp"

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

DEFINE_bool(telemetry_enabled, false,
            "Set to true to enable telemetry. We collect information about the "
            "running system (CPU and memory information) and information about "
            "the database runtime (vertex and edge counts and resource usage) "
            "to allow for easier improvement of the product.");

// Audit logging flags.
DEFINE_bool(audit_enabled, false, "Set to true to enable audit logging.");
DEFINE_VALIDATED_int32(audit_buffer_size, audit::kBufferSizeDefault,
                       "Maximum number of items in the audit log buffer.",
                       FLAG_IN_RANGE(1, INT32_MAX));
DEFINE_VALIDATED_int32(
    audit_buffer_flush_interval_ms, audit::kBufferFlushIntervalMillisDefault,
    "Interval (in milliseconds) used for flushing the audit log buffer.",
    FLAG_IN_RANGE(10, INT32_MAX));

using ServerT = communication::Server<BoltSession, SessionData>;
using communication::ServerContext;

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

  auto durability_directory =
      std::experimental::filesystem::path(FLAGS_durability_directory);

  auth::Auth auth{durability_directory / "auth"};

  audit::Log audit_log{durability_directory / "audit", FLAGS_audit_buffer_size,
                       FLAGS_audit_buffer_flush_interval_ms};
  if (FLAGS_audit_enabled) {
    audit_log.Start();
  }
  CHECK(utils::SignalHandler::RegisterHandler(
      utils::Signal::User2, [&audit_log]() { audit_log.ReopenLog(); }))
      << "Unable to register SIGUSR2 handler!";

  database::Master db;
  query::DistributedInterpreter interpreter(&db);
  SessionData session_data{&db, &interpreter, &auth, &audit_log};

  integrations::kafka::Streams kafka_streams{
      durability_directory / "streams",
      [&session_data](
          const std::string &query,
          const std::map<std::string, communication::bolt::Value> &params) {
        KafkaStreamWriter(session_data, query, params);
      }};

  try {
    // Recover possible streams.
    kafka_streams.Recover();
  } catch (const integrations::kafka::KafkaStreamException &e) {
    LOG(ERROR) << e.what();
  }

  session_data.interpreter->auth_ = &auth;
  session_data.interpreter->kafka_streams_ = &kafka_streams;

  ServerContext context;
  std::string service_name = "Bolt";
  if (FLAGS_key_file != "" && FLAGS_cert_file != "") {
    context = ServerContext(FLAGS_key_file, FLAGS_cert_file);
    service_name = "BoltS";
  }

  ServerT server({FLAGS_interface, static_cast<uint16_t>(FLAGS_port)},
                 &session_data, &context, FLAGS_session_inactivity_timeout,
                 service_name, FLAGS_num_workers);

  // Handler for regular termination signals
  auto shutdown = [&db] {
    // We call the shutdown method on the worker database so that we exit
    // cleanly.
    db.Shutdown();
  };

  InitSignalHandlers(shutdown);

  // Start the database.
  db.Start();

  // Start the Bolt server.
  CHECK(server.Start()) << "Couldn't start the Bolt server!";

  // The return code of `AwaitShutdown` is ignored because we want the database
  // to exit cleanly no matter what.
  db.AwaitShutdown([&server] {
    // Server needs to be shutdown first and then the database. This prevents a
    // race condition when a transaction is accepted during server shutdown.
    server.Shutdown();
    server.AwaitShutdown();
  });
}

void WorkerMain() {
  google::SetUsageMessage("Memgraph distributed worker");
  database::Worker db;

  // Handler for regular termination signals
  auto shutdown = [&db] {
    // We call the shutdown method on the worker database so that we exit
    // cleanly.
    db.Shutdown();
  };

  InitSignalHandlers(shutdown);

  // Start the database.
  db.Start();

  // The return code of `AwaitShutdown` is ignored because we want the database
  // to exit cleanly no matter what.
  db.AwaitShutdown();
}

int main(int argc, char **argv) {
  auto memgraph_main = [&]() {
    CHECK(!(FLAGS_master && FLAGS_worker))
        << "Can't run Memgraph as worker and master at the same time!";
    CHECK(FLAGS_master || FLAGS_worker)
        << "You must specify that Memgraph should be either a master or a worker!";
    if (FLAGS_master)
      MasterMain();
    else
      WorkerMain();
  };

  return WithInit(argc, argv, memgraph_main);
}
