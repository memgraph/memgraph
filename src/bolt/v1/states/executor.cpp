#include "executor.hpp"

#include "bolt/v1/messaging/codes.hpp"

namespace bolt
{

Executor::Executor() : logger(logging::log->logger("Executor")) {}

State* Executor::run(Session& session)
{
    // just read one byte that represents the struct type, we can skip the
    // information contained in this byte
    session.decoder.read_byte();

    auto message_type = session.decoder.read_byte();

    if(message_type == MessageCode::Run)
    {
        Query q;

        q.statement = session.decoder.read_string();

        this->run(session, q);
    }
    else if(message_type == MessageCode::PullAll)
    {
        pull_all(session);
    }
    else if(message_type == MessageCode::DiscardAll)
    {
        discard_all(session);
    }
    else if(message_type == MessageCode::Reset)
    {
        // todo rollback current transaction
        // discard all records waiting to be sent

        return this;
    }
    else
    {
        logger.error("Unrecognized message recieved");
        logger.debug("Invalid message type 0x{:02X}", message_type);

        return session.bolt.states.error.get();
    }

    return this;
}

void Executor::run(Session& session, Query& query)
{
    logger.trace("[Run] '{}'", query.statement);

    session.encoder.message_success();
    session.encoder.write_map_header(1);

    session.encoder.write_string("fields");
    session.encoder.write_list_header(1);
    session.encoder.write_string("name");

    session.encoder.flush();
}

void Executor::pull_all(Session& session)
{
    logger.trace("[PullAll]");

    session.encoder.message_record();
    session.encoder.write_list_header(1);
    session.encoder.write_string("buda");

    session.encoder.message_record();
    session.encoder.write_list_header(1);
    session.encoder.write_string("domko");

    session.encoder.message_record();
    session.encoder.write_list_header(1);
    session.encoder.write_string("max");

    session.encoder.message_success_empty();

    session.encoder.flush();
}

void Executor::discard_all(Session& session)
{
    logger.trace("[DiscardAll]");

    session.encoder.message_success();
    session.encoder.flush();
}

}
