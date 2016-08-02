#include <iostream>
#include <signal.h>

#include "bolt/v1/server/server.hpp"
#include "bolt/v1/server/worker.hpp"

#include "io/network/socket.hpp"

#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"

static bolt::Server<bolt::Worker>* serverptr;

Logger logger;

void sigint_handler(int s)
{
    auto signal = s == SIGINT ? "SIGINT" : "SIGABRT";

    logger.info("Recieved signal {}", signal);
    logger.info("Shutting down...");

    std::exit(EXIT_SUCCESS);
}

static constexpr const char* interface = "0.0.0.0";
static constexpr const char* port = "7687";

int main(void)
{
    logging::init_sync();
    logging::log->pipe(std::make_unique<Stdout>());
    logger = logging::log->logger("Main");

    signal(SIGINT, sigint_handler);
    signal(SIGABRT, sigint_handler);

    io::Socket socket;

    try
    {
        socket = io::Socket::bind(interface, port);
    }
    catch(io::NetworkError e)
    {
        logger.error("Cannot bind to socket on {} at {}", interface, port);
        logger.error("{}", e.what());

        std::exit(EXIT_FAILURE);
    }

    socket.set_non_blocking();
    socket.listen(1024);

    logger.info("Listening on {} at {}", interface, port);

    bolt::Server<bolt::Worker> server(std::move(socket));
    serverptr = &server;

    constexpr size_t N = 1;

    logger.info("Starting {} workers", N);
    server.start(N);

    logger.info("Shutting down...");

    return EXIT_SUCCESS;
}
