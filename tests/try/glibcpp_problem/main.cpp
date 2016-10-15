#include <iostream>

#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"

int main(void)
{
    // init logging
    logging::init_sync();
    logging::log->pipe(std::make_unique<Stdout>());

    // get Main logger
    Logger logger;
    logger = logging::log->logger("Main");
    logger.info("{}", logging::log->type());

    std::string* test = new std::string("test_value");

    return 0;
}
