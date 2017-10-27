#include "communication/reactor/reactor_distributed.hpp"

// reactor adress can't be 0.0.0.0.
DEFINE_string(reactor_address, "127.0.0.1", "Network server bind address");
DEFINE_int32(reactor_port, 10000, "Network server bind port");
