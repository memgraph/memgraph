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
#include "database/distributed_graph_db.hpp"
#include "database/graph_db.hpp"
#include "integrations/kafka/exceptions.hpp"
#include "integrations/kafka/streams.hpp"
#include "memgraph_init.hpp"
#include "query/distributed_interpreter.hpp"
#include "query/exceptions.hpp"
#include "telemetry/telemetry.hpp"
#include "utils/flag_validation.hpp"

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

DEFINE_bool(telemetry_enabled, false,
            "Set to true to enable telemetry. We collect information about the "
            "running system (CPU and memory information) and information about "
            "the database runtime (vertex and edge counts and resource usage) "
            "to allow for easier improvement of the product.");

using ServerT = communication::Server<BoltSession, SessionData>;
using communication::ServerContext;

void SingleNodeMain() {
  google::SetUsageMessage("Memgraph single-node database server");
  database::SingleNode db;
  query::Interpreter interpreter;
  SessionData session_data{&db, &interpreter};

  integrations::kafka::Streams kafka_streams{
      std::experimental::filesystem::path(FLAGS_durability_directory) /
          "streams",
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

  session_data.interpreter->auth_ = &session_data.auth;
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

  // Setup telemetry
  std::experimental::optional<telemetry::Telemetry> telemetry;
  if (FLAGS_telemetry_enabled) {
    telemetry.emplace(
        "https://telemetry.memgraph.com/88b5e7e8-746a-11e8-9f85-538a9e9690cc/",
        std::experimental::filesystem::path(FLAGS_durability_directory) /
            "telemetry",
        std::chrono::minutes(10));
    telemetry->AddCollector("db", [&db]() -> nlohmann::json {
      auto dba = db.Access();
      return {{"vertices", dba->VerticesCount()}, {"edges", dba->EdgesCount()}};
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
  query::DistributedInterpreter interpreter(&db);
  SessionData session_data{&db, &interpreter};

  integrations::kafka::Streams kafka_streams{
      std::experimental::filesystem::path(FLAGS_durability_directory) /
          "streams",
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

  session_data.interpreter->auth_ = &session_data.auth;
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
