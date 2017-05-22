#include <signal.h>
#include <iostream>

#include "gflags/gflags.h"

#include "dbms/dbms.hpp"
#include "query/engine.hpp"

#include "communication/bolt/v1/session.hpp"
#include "communication/server.hpp"

#include "io/network/network_endpoint.hpp"
#include "io/network/network_error.hpp"
#include "io/network/socket.hpp"

#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"

#include "utils/config/config.hpp"
#include "utils/signals/handler.hpp"
#include "utils/stacktrace/log.hpp"
#include "utils/terminate_handler.hpp"

using endpoint_t = io::network::NetworkEndpoint;
using socket_t = io::network::Socket;
using session_t = communication::bolt::Session<socket_t>;
using result_stream_t =
    communication::bolt::ResultStream<communication::bolt::Encoder<
        communication::bolt::ChunkedEncoderBuffer<socket_t>>>;
using bolt_server_t =
    communication::Server<session_t, result_stream_t, socket_t>;

static bolt_server_t *serverptr;

Logger logger;

DEFINE_string(interface, "0.0.0.0", "Default interface on which to listen.");
DEFINE_string(port, "7687", "Default port on which to listen.");

void throw_and_stacktace(std::string message) {
  Stacktrace stacktrace;
  logger.info(stacktrace.dump());
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
// logging init
#ifdef SYNC_LOGGER
  logging::init_sync();
#else
  logging::init_async();
#endif
  logging::log->pipe(std::make_unique<Stdout>());

  // get logger
  logger = logging::log->logger("Main");
  logger.info("{}", logging::log->type());

  // unhandled exception handler init
  std::set_terminate(&terminate_handler);

  // signal handling init
  SignalHandler::register_handler(Signal::SegmentationFault, []() {
    log_stacktrace("SegmentationFault signal raised");
    std::exit(EXIT_FAILURE);
  });
  SignalHandler::register_handler(Signal::Terminate, []() {
    log_stacktrace("Terminate signal raised");
    std::exit(EXIT_FAILURE);
  });
  SignalHandler::register_handler(Signal::Abort, []() {
    log_stacktrace("Abort signal raised");
    std::exit(EXIT_FAILURE);
  });

  // register args
  CONFIG_REGISTER_ARGS(argc, argv);

  // initialize endpoint
  endpoint_t endpoint;
  try {
    endpoint = endpoint_t(FLAGS_interface, FLAGS_port);
  } catch (io::network::NetworkEndpointException &e) {
    logger.error("{}", e.what());
    std::exit(EXIT_FAILURE);
  }

  // initialize socket
  socket_t socket;
  if (!socket.Bind(endpoint)) {
    logger.error("Cannot bind to socket on {} at {}", FLAGS_interface,
                 FLAGS_port);
    std::exit(EXIT_FAILURE);
  }
  if (!socket.SetNonBlocking()) {
    logger.error("Cannot set socket to non blocking!");
    std::exit(EXIT_FAILURE);
  }
  if (!socket.Listen(1024)) {
    logger.error("Cannot listen on socket!");
    std::exit(EXIT_FAILURE);
  }

  logger.info("Listening on {} at {}", FLAGS_interface, FLAGS_port);

  Dbms dbms;
  QueryEngine<result_stream_t> query_engine;

  // initialize server
  bolt_server_t server(std::move(socket), dbms, query_engine);
  serverptr = &server;

  // server start with N threads
  // TODO: N should be configurable
  auto N = std::thread::hardware_concurrency();
  logger.info("Starting {} workers", N);
  server.Start(N);

  logger.info("Shutting down...");
  return EXIT_SUCCESS;
}
