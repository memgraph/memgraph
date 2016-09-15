#pragma once

#include "utils/exceptions/basic_exception.hpp"

class QueryEngineException : public BasicException
{
public:
    using BasicException::BasicException;
};

class CppGeneratorException : public BasicException
{
public:
    using BasicException::BasicException;
};

class DecoderException : public BasicException
{
public:
    using BasicException::BasicException;
};

class NotYetImplemented : public BasicException
{
public:
    using BasicException::BasicException;
};

class NonExhaustiveSwitch : public BasicException
{
public:
    using BasicException::BasicException;
};

class OutOfMemory : public BasicException
{
public:
    using BasicException::BasicException;
};
