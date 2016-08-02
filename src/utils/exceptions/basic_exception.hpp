#pragma once

#include <stdexcept>
#include <fmt/format.h>

#include "utils/auto_scope.hpp"
#include "utils/stacktrace.hpp"

class BasicException : public std::exception
{
public:
    BasicException(const std::string& message) noexcept : message(message)
    {
#ifndef NDEBUG
        this->message += '\n';

        Stacktrace stacktrace;

        for(auto& line : stacktrace)
            this->message += fmt::format("  at {} ({})\n",
                line.function, line.location);
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
};

