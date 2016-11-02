#pragma once

#include "utils/exceptions/basic_exception.hpp"

class PlanCompilationException : public BasicException
{
public:
    using BasicException::BasicException;
};
