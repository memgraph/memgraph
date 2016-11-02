#pragma once

#include "utils/exceptions/basic_exception.hpp"

class QueryEngineException : public BasicException
{
public:
    using BasicException::BasicException;
};
