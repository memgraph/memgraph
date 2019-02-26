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

using ServerT = communication::Server<BoltSession, SessionData>;
using communication::ServerContext;

void SingleNodeHAMain() {
  google::SetUsageMessage(
      "Memgraph high availability single-node database server");

  auto durability_directory =
      std::experimental::filesystem::path(FLAGS_durability_directory);

  database::GraphDb db;
  query::Interpreter interpreter;
  SessionData session_data{&db, &interpreter, nullptr, nullptr};

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
    db.Shutdown();
  };

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
  return WithInit(argc, argv, SingleNodeHAMain);
}
