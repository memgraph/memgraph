#include "error.hpp"

#include "bolt/v1/session.hpp"

namespace bolt
{

State* Error::run(Session& session)
{
    auto message_type = session.decoder.read_byte();

    if(message_type == MessageCode::AckFailure)
    {
        // todo reset current statement? is it even necessary?

        return session.bolt.states.executor.get();
    }
    else if(message_type == MessageCode::Reset)
    {
        // todo rollback current transaction
        // discard all records waiting to be sent

        return session.bolt.states.executor.get();
    }

    session.encoder.message_ignored();
    session.encoder.flush();

    return this;
}

}
