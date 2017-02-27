#pragma once

// the flag is only used in hardcoded queries compilation
//     see usage in plan_compiler.hpp
#ifdef HARDCODED_OUTPUT_STREAM
#include "communication/bolt/communication.hpp"
using Stream = communication::OutputStream;
#else
#include "../stream/print_record_stream.hpp"
using Stream = PrintRecordStream;
#endif
