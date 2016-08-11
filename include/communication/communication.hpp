#pragma once

#include "io/network/socket.hpp"
#include "communication/bolt/v1/serialization/record_stream.hpp"

namespace communication
{
    using OutputStream = bolt::RecordStream<io::Socket>;
}
