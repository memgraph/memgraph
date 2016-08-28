#include "communication/bolt/v1/states/init.hpp"

#include "communication/bolt/v1/session.hpp"
#include "communication/bolt/v1/messaging/codes.hpp"

#include "utils/likely.hpp"

namespace bolt
{

Init::Init() : MessageParser<Init>(logging::log->logger("Init")) {}

State* Init::parse(Session& session, Message& message)
{
    auto struct_type = session.decoder.read_byte();

    if(UNLIKELY(struct_type != 0xB2))
    {
        logger.debug("{}", struct_type);

        logger.debug("Expected struct marker 0xB2 instead of 0x{:02X}",
                     (unsigned)struct_type);

        return nullptr;
    }

    auto message_type = session.decoder.read_byte();

    if(UNLIKELY(message_type != MessageCode::Init))
    {
        logger.debug("Expected Init (0x01) instead of (0x{:02X})",
                     (unsigned)message_type);

        return nullptr;
    }

    message.client_name = session.decoder.read_string();

    // TODO read authentication tokens

    return this;
}

State* Init::execute(Session& session, Message& message)
{
    logger.debug("Client connected '{}'", message.client_name);

    session.output_stream.write_success_empty();
    session.output_stream.chunk();
    session.output_stream.send();

    return session.bolt.states.executor.get();
}

}
