#pragma once

#include "utils/exceptions/basic_exception.hpp"

class PlanExecutionException : public BasicException
{
public:
    using BasicException::BasicException;
};
