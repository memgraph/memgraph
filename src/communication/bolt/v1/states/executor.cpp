#include "communication/bolt/v1/states/executor.hpp"
#include "communication/bolt/v1/messaging/codes.hpp"
#include "database/graph_db_accessor.hpp"

#ifdef BARRIER
#include "barrier/barrier.cpp"
#endif

namespace bolt
{

Executor::Executor() : State(logging::log->logger("Executor")) {}

State *Executor::run(Session &session)
{
    // just read one byte that represents the struct type, we can skip the
    // information contained in this byte
    session.decoder.read_byte();

    logger.debug("Run");

    auto message_type = session.decoder.read_byte();

    if (message_type == MessageCode::Run)
    {
        Query q;

        q.statement = session.decoder.read_string();

        try
        {
            return this->run(session, q);
            // TODO: RETURN success MAYBE
        }
        catch (const QueryEngineException &e)
        {
            session.output_stream.write_failure(
                {{"code", "Memgraph.QueryEngineException"},
                 {"message", e.what()}});
            session.output_stream.send();
            return session.bolt.states.error.get();
        } catch (std::exception &e) {
            session.output_stream.write_failure(
                {{"code", "Memgraph.Exception"},
                 {"message", e.what()}});
            session.output_stream.send();
            return session.bolt.states.error.get();
        }
    }
    else if (message_type == MessageCode::PullAll)
    {
        pull_all(session);
    }
    else if (message_type == MessageCode::DiscardAll)
    {
        discard_all(session);
    }
    else if (message_type == MessageCode::Reset)
    {
        // TODO: rollback current transaction
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

State *Executor::run(Session &session, Query &query)
{
    logger.trace("[Run] '{}'", query.statement);

    auto db_accessor = session.active_db();
    logger.debug("[ActiveDB] '{}'", db_accessor.name());

    auto is_successfully_executed =
        query_engine.Run(query.statement, db_accessor, session.output_stream);

    if (!is_successfully_executed)
    {
        session.output_stream.write_failure(
            {{"code", "Memgraph.QueryExecutionFail"},
             {"message", "Query execution has failed (probably there is no "
                         "element or there are some problems with concurrent "
                         "access -> client has to resolve problems with "
                         "concurrent access)"}});
        session.output_stream.send();
        return session.bolt.states.error.get();
    }

    return this;
}

void Executor::pull_all(Session &session)
{
    logger.trace("[PullAll]");

    session.output_stream.send();
}

void Executor::discard_all(Session &session)
{
    logger.trace("[DiscardAll]");

    // TODO: discard state

    session.output_stream.write_success();
    session.output_stream.chunk();
    session.output_stream.send();
}
}
