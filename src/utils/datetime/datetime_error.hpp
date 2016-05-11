#pragma once

#include "utils/exceptions/basic_exception.hpp"

class DatetimeError : public BasicException
{
public:
    using BasicException::BasicException;
};

