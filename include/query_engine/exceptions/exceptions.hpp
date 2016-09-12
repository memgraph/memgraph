#pragma once

#include "utils/exceptions/basic_exception.hpp"

class QueryEngineException : public BasicException
{
    using BasicException::BasicException;
};

class CppGeneratorException : public BasicException
{
    using BasicException::BasicException;
};

class DecoderException : public BasicException
{
    using BasicException::BasicException;
};

class NotYetImplemented : public BasicException
{
    using BasicException::BasicException;
};

class NonExhaustiveSwitch : public BasicException
{
    using BasicException::BasicException;
};
