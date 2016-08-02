#pragma once

#include "bolt/v1/states/message_parser.hpp"

namespace bolt
{

class Init : public MessageParser<Init>
{
public:
    struct Message
    {
        std::string client_name;
    };

    Init();

    State* parse(Session& session, Message& message);
    State* execute(Session& session, Message& message);
};

}
