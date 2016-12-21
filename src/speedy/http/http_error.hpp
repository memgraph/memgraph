#pragma once

#include <stdexcept>
#include <string>

namespace http
{

class HttpError : public std::runtime_error
{
public:
    explicit HttpError(const std::string& message)
        : std::runtime_error(message) {}
};

}

#endif
