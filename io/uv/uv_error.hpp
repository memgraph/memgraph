#pragma once

#include <stdexcept>
#include <string>

namespace uv
{

class UvError : public std::runtime_error
{
public:
    UvError(const std::string& message)
        : std::runtime_error(message) {}
};

}
