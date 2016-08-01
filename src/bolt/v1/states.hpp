#pragma once

#include "states/state.hpp"
#include "logging/log.hpp"

namespace bolt
{

class States
{
public:
    States();

    State::uptr handshake;
    State::uptr init;
    State::uptr executor;
};

}
