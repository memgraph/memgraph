#pragma once

#include "bolt/v1/transport/chunked_encoder.hpp"
#include "bolt/v1/transport/socket_stream.hpp"

#include "bolt/v1/transport/bolt_encoder.hpp"

namespace bolt
{

template <class Socket>
class SocketSerializer : public BoltEncoder<ChunkedEncoder<SocketStream>>
{
public:
    SocketSerializer(Socket& socket) : BoltEncoder(encoder), stream(socket) {}

private:
    SocketStream stream;
    ChunkedEncoder<SocketStream> encoder {stream};
};

}
