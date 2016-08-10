#include "bolt/v1/states/error.hpp"

namespace bolt
{

State* Error::run(Session& session)
{
    auto message_type = session.decoder.read_byte();

    if(message_type == MessageCode::AckFailure)
    {
        // TODO reset current statement? is it even necessary?

        session.encoder.message_success_empty();
        session.encoder.flush();

        return session.bolt.states.executor.get();
    }
    else if(message_type == MessageCode::Reset)
    {
        // TODO rollback current transaction
        // discard all records waiting to be sent

        session.encoder.message_success_empty();
        session.encoder.flush();

        return session.bolt.states.executor.get();
    }

    session.encoder.message_ignored();
    session.encoder.flush();

    return this;
}

}
