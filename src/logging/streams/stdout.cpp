#include "logging/streams/stdout.hpp"

#include <iostream>
#include "logging/streams/format.hpp"

void Stdout::emit(const Log::Record& record) {
  std::cout << logging::Formatter::format(logging::format::out, record);
}
