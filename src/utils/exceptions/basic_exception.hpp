#pragma once

#include <stdexcept>

#include <cppformat/format.h>

#include "utils/auto_scope.hpp"
#include "utils/stacktrace.hpp"

class BasicException : public std::exception
{
public:
    template <class... Args>
    BasicException(Args&&... args) noexcept
        : message(fmt::format(std::forward<Args>(args)...))
    {
#ifndef NDEBUG
        message += '\n';

        Stacktrace stacktrace;

        for(auto& line : stacktrace)
            message += fmt::format("  at {} ({})\n",
                line.function, line.location);
#endif
    }

    const char* what() const noexcept override
    {
        return message.c_str();
    }

private:
    std::string message;
};


