#pragma once

#include "utils/exceptions/basic_exception.hpp"

class NotYetImplemented : public BasicException
{
public:
    using BasicException::BasicException;
};
