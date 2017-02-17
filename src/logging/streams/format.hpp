#pragma once

#include <fmt/format.h>
#include <string>

#include "logging/log.hpp"

// TODO: in c++17 replace with logging::format
namespace logging
{
    namespace format
    {
        static const std::string out = "{} {:<5} [{}] {}\n";
        static const std::string err = out;

        // TODO: configurable formats
    }

    class Formatter
    {
    public:
        static std::string format(const std::string &format,
                                  const Log::Record &record)
        {
            return fmt::format(format, static_cast<std::string>(record.when()),
                               record.level_str(), record.where(),
                               record.text());
        }
    };
}
