#pragma once

#include "utils/exceptions/basic_exception.hpp"

class QueryEngineException : public BasicException
{
    using BasicException::BasicException;
};
