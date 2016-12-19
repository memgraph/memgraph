#include <iostream>
#include <signal.h>

#include "communication/bolt/v1/server/server.hpp"
#include "communication/bolt/v1/server/worker.hpp"

#include "io/network/socket.hpp"

#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"

#include "utils/signals/handler.hpp"
#include "utils/terminate_handler.hpp"
#include "utils/stacktrace/log.hpp"

static bolt::Server<bolt::Worker> *serverptr;

Logger logger;

// TODO: load from configuration
static constexpr const char *interface = "0.0.0.0";
static constexpr const char *port      = "7687";

int main(void)
{
// logging init
#ifdef SYNC_LOGGER
    logging::init_sync();
#else
    logging::init_async();
#endif
    logging::log->pipe(std::make_unique<Stdout>());

    // logger init
    logger = logging::log->logger("Main");
    logger.info("{}", logging::log->type());

    // unhandled exception handler
    std::set_terminate(&terminate_handler);

    // signal handling
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

    // initialize socket
    io::Socket socket;
    try
    {
        socket = io::Socket::bind(interface, port);
    }
    catch (io::NetworkError e)
    {
        logger.error("Cannot bind to socket on {} at {}", interface, port);
        logger.error("{}", e.what());
        std::exit(EXIT_FAILURE);
    }
    socket.set_non_blocking();
    socket.listen(1024);
    logger.info("Listening on {} at {}", interface, port);

    // initialize server
    bolt::Server<bolt::Worker> server(std::move(socket));
    serverptr = &server;

    // server start with N threads
    // TODO: N should be configurable
    auto N = std::thread::hardware_concurrency();
    logger.info("Starting {} workers", N);
    server.start(N);

    logger.info("Shutting down...");
    return EXIT_SUCCESS;
}
