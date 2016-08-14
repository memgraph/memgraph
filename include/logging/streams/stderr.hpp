#pragma once

#include "logging/log.hpp"

class Stderr : public Log::Stream
{
public:
    void emit(const Log::Record&) override;
};
