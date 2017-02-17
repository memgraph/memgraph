#pragma once

#include "logging/log.hpp"

class Stdout : public Log::Stream
{
public:
    void emit(const Log::Record&) override;
};
