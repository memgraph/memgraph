#include "stdout.hpp"

#include <cppformat/format.h>

void Stdout::emit(const Log::Record& record)
{
    fmt::print("{} {:<5} [{}] {}\n", record.when(), record.level_str(),
               record.where(), record.text());
}

