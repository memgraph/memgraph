#include "logging/streams/stderr.hpp"

#include <iostream>
#include "logging/streams/format.hpp"

void Stderr::emit(const Log::Record& record)
{
    std::cerr << logging::Formatter::format(logging::format::err, record);
}
