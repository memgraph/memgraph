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
#include "database/single_node_ha/graph_db.hpp"
#include "memgraph_init.hpp"
#include "query/exceptions.hpp"
#include "utils/flag_validation.hpp"

// General purpose flags.
DEFINE_string(bolt_address, "0.0.0.0",
              "IP address on which the Bolt server should listen.");
DEFINE_VALIDATED_int32(bolt_port, 7687,
                       "Port on which the Bolt server should listen.",
                       FLAG_IN_RANGE(0, std::numeric_limits<uint16_t>::max()));
DEFINE_VALIDATED_int32(
    bolt_num_workers, std::max(std::thread::hardware_concurrency(), 1U),
    "Number of workers used by the Bolt server. By default, this will be the "
    "number of processing units available on the machine.",
    FLAG_IN_RANGE(1, INT32_MAX));
DEFINE_VALIDATED_int32(
    bolt_session_inactivity_timeout, 1800,
    "Time in seconds after which inactive Bolt sessions will be "
    "closed.",
    FLAG_IN_RANGE(1, INT32_MAX));
DEFINE_string(bolt_cert_file, "",
              "Certificate file which should be used for the Bolt server.");
DEFINE_string(bolt_key_file, "",
              "Key file which should be used for the Bolt server.");

using ServerT = communication::Server<BoltSession, SessionData>;
using communication::ServerContext;

void SingleNodeHAMain() {
  auto durability_directory = std::filesystem::path(FLAGS_durability_directory);

  database::GraphDb db;
  query::InterpreterContext interpreter_context{&db};
  SessionData session_data{&db, &interpreter_context, nullptr, nullptr};

  ServerContext context;
  std::string service_name = "Bolt";
  if (!FLAGS_bolt_key_file.empty() && !FLAGS_bolt_cert_file.empty()) {
    context = ServerContext(FLAGS_bolt_key_file, FLAGS_bolt_cert_file);
    service_name = "BoltS";
  }

  ServerT server({FLAGS_bolt_address, static_cast<uint16_t>(FLAGS_bolt_port)},
                 &session_data, &context, FLAGS_bolt_session_inactivity_timeout,
                 service_name, FLAGS_bolt_num_workers);

  // Handler for regular termination signals
  auto shutdown = [&db] { db.Shutdown(); };

  InitSignalHandlers(shutdown);

  // Start the database.
  db.Start();
  // Start the Bolt server.
  CHECK(server.Start()) << "Couldn't start the Bolt server!";

  db.AwaitShutdown([&server] {
    server.Shutdown();
    server.AwaitShutdown();
  });
}

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph high availability database server");
  return WithInit(argc, argv, SingleNodeHAMain);
}
