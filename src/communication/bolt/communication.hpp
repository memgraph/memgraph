#pragma once

#include "communication/bolt/v1/serialization/record_stream.hpp"
#include "io/network/socket.hpp"

namespace communication {
using OutputStream = bolt::RecordStream<io::Socket>;
}
