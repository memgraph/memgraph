#include "bolt.hpp"

#include "session.hpp"
#include <iostream>

namespace bolt
{

Bolt::Bolt()
{
}

Session* Bolt::create_session(io::Socket&& socket)
{
    // TODO fix session lifecycle handling
    // dangling pointers are not cool :)

    return new Session(std::forward<io::Socket>(socket), *this);
}

void Bolt::close(Session* session)
{
    session->socket.close();
}

}
