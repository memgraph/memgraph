#include "bolt/v1/bolt.hpp"

#include "bolt/v1/session.hpp"
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
    
    // TODO attach currently active Db

    return new Session(std::forward<io::Socket>(socket), *this);
}

void Bolt::close(Session* session)
{
    session->socket.close();
}

}
