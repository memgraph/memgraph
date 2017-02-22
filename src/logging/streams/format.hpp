#pragma once

#include <fmt/format.h>
#include <string>

#include "logging/log.hpp"

namespace logging::format {
// TODO: read formats from the config
static const std::string out = "{} {:<5} [{}] {}\n";
static const std::string err = out;
}
namespace logging {
class Formatter {
 public:
  static std::string format(const std::string &format,
                            const Log::Record &record) {
    return fmt::format(format, static_cast<std::string>(record.when()),
                       record.level_str(), record.where(), record.text());
  }
};
}
