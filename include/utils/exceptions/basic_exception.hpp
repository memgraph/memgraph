#pragma once

#include <stdexcept>
#include <fmt/format.h>

#include "utils/auto_scope.hpp"
#include "utils/stacktrace.hpp"

class BasicException : public std::exception
{
public:
    BasicException(const std::string& message) noexcept
        : message(message),
          stacktrace_size(3)
    {
#ifndef NDEBUG
        this->message += '\n';

        Stacktrace stacktrace;

        // TODO: write this better
        // (limit the size of stacktrace)
        uint64_t count = 0;

        for(auto& line : stacktrace) {
            this->message += fmt::format("  at {} ({})\n",
                line.function, line.location);

            if (++count >= stacktrace_size)
                break;
        }
#endif
    }

    template <class... Args>
    BasicException(const std::string& format, Args&&... args) noexcept
        : BasicException(fmt::format(format, std::forward<Args>(args)...)) {}

    const char* what() const noexcept override
    {
        return message.c_str();
    }

private:
    std::string message;
    uint64_t stacktrace_size;
};

