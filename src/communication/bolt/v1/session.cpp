#include "communication/bolt/v1/session.hpp"

namespace bolt
{

Session::Session(io::Socket &&socket, Bolt &bolt)
    : Stream(std::forward<io::Socket>(socket)), bolt(bolt)
{
    logger = logging::log->logger("Session");

    // start with a handshake state
    state = bolt.states.handshake.get();
}

bool Session::alive() const { return state != nullptr; }

void Session::execute(const byte *data, size_t len)
{
    // mark the end of the message
    auto end = data + len;

    while (true) {
        auto size = end - data;

        if (LIKELY(connected)) {
            logger.debug("Decoding chunk of size {}", size);
            auto finished = decoder.decode(data, size);

            if (!finished) return;
        } else {
            logger.debug("Decoding handshake of size {}", size);
            decoder.handshake(data, size);
        }

        state = state->run(*this);
        decoder.reset();
    }
}

void Session::close()
{
    logger.debug("Closing session");
    bolt.close(this);
}

GraphDb &Session::active_db() { return bolt.dbms.active(); }
}
