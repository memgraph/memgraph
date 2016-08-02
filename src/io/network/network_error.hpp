#pragma once

#include <stdexcept>

#include "utils/exceptions/basic_exception.hpp"

namespace io
{

class NetworkError : public BasicException
{
public:
    using BasicException::BasicException;
};

}
