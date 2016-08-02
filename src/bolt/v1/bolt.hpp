#pragma once

#include "states.hpp"
#include "io/network/socket.hpp"

namespace bolt
{

class Session;

class Bolt
{
    friend class Session;

public:
    Bolt();

    Session* create_session(io::Socket&& socket);
    void close(Session* session);

    States states;
};

}
