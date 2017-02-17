#pragma once

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <stdexcept>

#include "utils/auto_scope.hpp"
#include "utils/stacktrace/stacktrace.hpp"

class BasicException : public std::exception
{
public:
    BasicException(const std::string &message) noexcept : message_(message)
    {
        Stacktrace stacktrace;
        message_.append(stacktrace.dump());
    }

    template <class... Args>
    BasicException(const std::string &format, Args &&... args) noexcept
        : BasicException(fmt::format(format, std::forward<Args>(args)...))
    {
    }

    template <class... Args>
    BasicException(const char* format, Args &&... args) noexcept
        : BasicException(fmt::format(std::string(format), std::forward<Args>(args)...))
    {
    }

    const char *what() const noexcept override { return message_.c_str(); }

private:
    std::string message_;
};
