#pragma once

#include "io/network/socket.hpp"
#include "io/network/tcp/stream.hpp"

#include "bolt/v1/bolt.hpp"
#include "bolt/v1/serialization/record_stream.hpp"
#include "bolt/v1/states/state.hpp"
#include "bolt/v1/transport/bolt_decoder.hpp"
#include "bolt/v1/transport/bolt_encoder.hpp"

#include "logging/default.hpp"

namespace bolt
{

class Session : public io::tcp::Stream<io::Socket>
{
public:
    using Decoder = BoltDecoder;
    using OutputStream = RecordStream<io::Socket>;

    Session(io::Socket &&socket, Bolt &bolt);

    bool alive() const;

    void execute(const byte *data, size_t len);
    void close();

    Bolt &bolt;
    Db &active_db();

    Decoder decoder;
    OutputStream output_stream{socket};

    bool connected{false};
    State *state;

protected:
    Logger logger;
};
}
