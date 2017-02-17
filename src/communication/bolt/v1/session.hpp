#pragma once

#include "io/network/socket.hpp"
#include "io/network/tcp/stream.hpp"

#include "communication/bolt/v1/bolt.hpp"
#include "communication/bolt/v1/serialization/record_stream.hpp"
#include "communication/bolt/v1/states/state.hpp"
#include "communication/bolt/v1/transport/bolt_decoder.hpp"
#include "communication/bolt/v1/transport/bolt_encoder.hpp"
#include "communication/bolt/communication.hpp"

#include "logging/default.hpp"

namespace bolt {

  class Session : public io::tcp::Stream<io::Socket> {
  public:
    using Decoder = BoltDecoder;
    using OutputStream = communication::OutputStream;

    Session(io::Socket &&socket, Bolt &bolt);

    bool alive() const;

    void execute(const byte *data, size_t len);

    void close();

    Bolt &bolt;

    GraphDbAccessor active_db();

    Decoder decoder;
    OutputStream output_stream{socket};

    bool connected{false};
    State *state;

  protected:
    Logger logger;
  };
}
