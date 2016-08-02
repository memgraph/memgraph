#include "handshake.hpp"

#include "bolt/v1/session.hpp"

namespace bolt
{

static constexpr uint32_t preamble = 0x6060B017;

static constexpr byte protocol[4] = {0x00, 0x00, 0x00, 0x01};

State* Handshake::run(Session& session)
{
    if(UNLIKELY(session.decoder.read_uint32() != preamble))
        return nullptr;

    // TODO so far we only support version 1 of the protocol so it doesn't
    // make sense to check which version the client prefers
    // this will change in the future

    session.connected = true;
    session.socket.write(protocol, sizeof protocol);

    return session.bolt.states.init.get();
}

}
