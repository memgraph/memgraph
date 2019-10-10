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
#ifdef MG_SINGLE_NODE_V2
#include "storage/v2/storage.hpp"
#else
#include "database/single_node/graph_db.hpp"
#endif
#include "integrations/kafka/exceptions.hpp"
#include "integrations/kafka/streams.hpp"
#include "memgraph_init.hpp"
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

void SingleNodeMain() {
  google::SetUsageMessage("Memgraph single-node database server");

  // All enterprise features should be constructed before the main database
  // storage. This will cause them to be destructed *after* the main database
  // storage. That way any errors that happen during enterprise features
  // destruction won't have an impact on the storage engine.
  // Example: When the main storage is destructed it makes a snapshot. When
  // audit logging is destructed it syncs all pending data to disk and that can
  // fail. That is why it must be destructed *after* the main database storage
  // to minimise the impact of their failure on the main storage.

  // Begin enterprise features initialization

  auto durability_directory = std::filesystem::path(FLAGS_durability_directory);

  // Auth
  auth::Auth auth{durability_directory / "auth"};

  // Audit log
  audit::Log audit_log{durability_directory / "audit", FLAGS_audit_buffer_size,
                       FLAGS_audit_buffer_flush_interval_ms};
  // Start the log if enabled.
  if (FLAGS_audit_enabled) {
    audit_log.Start();
  }
  // Setup SIGUSR2 to be used for reopening audit log files, when e.g. logrotate
  // rotates our audit logs.
  CHECK(utils::SignalHandler::RegisterHandler(
      utils::Signal::User2, [&audit_log]() { audit_log.ReopenLog(); }))
      << "Unable to register SIGUSR2 handler!";

  // End enterprise features initialization

  // Main storage and execution engines initialization

#ifdef MG_SINGLE_NODE_V2
  storage::Storage db;
#else
  database::GraphDb db;
#endif
  query::InterpreterContext interpreter_context{&db};
  SessionData session_data{&db, &interpreter_context, &auth, &audit_log};

  integrations::kafka::Streams kafka_streams{
      durability_directory / "streams",
      [&session_data](
          const std::string &query,
          const std::map<std::string, communication::bolt::Value> &params) {
        KafkaStreamWriter(session_data, query, params);
      }};

  interpreter_context.auth = &auth;
  interpreter_context.kafka_streams = &kafka_streams;

  try {
    // Recover possible streams.
    kafka_streams.Recover();
  } catch (const integrations::kafka::KafkaStreamException &e) {
    LOG(ERROR) << e.what();
  }

  ServerContext context;
  std::string service_name = "Bolt";
  if (FLAGS_key_file != "" && FLAGS_cert_file != "") {
    context = ServerContext(FLAGS_key_file, FLAGS_cert_file);
    service_name = "BoltS";
  }

  ServerT server({FLAGS_interface, static_cast<uint16_t>(FLAGS_port)},
                 &session_data, &context, FLAGS_session_inactivity_timeout,
                 service_name, FLAGS_num_workers);

  // Setup telemetry
  std::optional<telemetry::Telemetry> telemetry;
#ifndef MG_SINGLE_NODE_V2
  if (FLAGS_telemetry_enabled) {
    telemetry.emplace(
        "https://telemetry.memgraph.com/88b5e7e8-746a-11e8-9f85-538a9e9690cc/",
        durability_directory / "telemetry", std::chrono::minutes(10));
    telemetry->AddCollector("db", [&db]() -> nlohmann::json {
      auto dba = db.Access();
      return {{"vertices", dba.VerticesCount()}, {"edges", dba.EdgesCount()}};
    });
  }
#endif

  // Handler for regular termination signals
  auto shutdown = [&server] {
    // Server needs to be shutdown first and then the database. This prevents a
    // race condition when a transaction is accepted during server shutdown.
    server.Shutdown();
  };
  InitSignalHandlers(shutdown);

  CHECK(server.Start()) << "Couldn't start the Bolt server!";
  server.AwaitShutdown();
}

int main(int argc, char **argv) { return WithInit(argc, argv, SingleNodeMain); }
