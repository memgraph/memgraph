#include "communication/bolt/v1/states/executor.hpp"
#include "communication/bolt/v1/messaging/codes.hpp"

namespace bolt
{

Executor::Executor() : logger(logging::log->logger("Executor")) {}

State* Executor::run(Session& session)
{
    // just read one byte that represents the struct type, we can skip the
    // information contained in this byte
    session.decoder.read_byte();

    logger.debug("Run");

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

    auto &db = session.active_db();
    logger.debug("[ActiveDB] '{}'", db.name());

    // TODO: hangle syntax error use case
    query_engine.execute(query.statement, db, session.output_stream);
}

void Executor::pull_all(Session& session)
{
    logger.trace("[PullAll]");

    session.output_stream.send();
}

void Executor::discard_all(Session& session)
{
    logger.trace("[DiscardAll]");

    // TODO: discard state

    session.output_stream.write_success();
    session.output_stream.chunk();
    session.output_stream.send();
}

}
