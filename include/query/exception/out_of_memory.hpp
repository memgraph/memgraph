#pragma once

#include "utils/exceptions/basic_exception.hpp"

class OutOfMemory : public BasicException
{
public:
    using BasicException::BasicException;
};
