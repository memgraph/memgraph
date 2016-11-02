#pragma once

#include "utils/exceptions/basic_exception.hpp"

class NonExhaustiveSwitch : public BasicException
{
public:
    using BasicException::BasicException;
};
