#pragma once

#include <string>

namespace web
{

class Logger
{
public:

    Logger() { /* create connection */ }

    // TODO: singleton
    
    void up_ping();
    void send(const std::string&);
    void down_ping();
};

}
