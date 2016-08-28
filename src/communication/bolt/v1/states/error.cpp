#include "communication/bolt/v1/states/error.hpp"

namespace bolt
{

State* Error::run(Session& session)
{
    auto message_type = session.decoder.read_byte();

    if(message_type == MessageCode::AckFailure)
    {
        // TODO reset current statement? is it even necessary?
        
        session.output_stream.write_success_empty();
        session.output_stream.chunk();
        session.output_stream.send();

        return session.bolt.states.executor.get();
    }
    else if(message_type == MessageCode::Reset)
    {
        // TODO rollback current transaction
        // discard all records waiting to be sent

        session.output_stream.write_success_empty();
        session.output_stream.chunk();
        session.output_stream.send();

        return session.bolt.states.executor.get();
    }

    // TODO: write this as single call
    session.output_stream.write_ignored();
    session.output_stream.chunk();
    session.output_stream.send();

    return this;
}

}
