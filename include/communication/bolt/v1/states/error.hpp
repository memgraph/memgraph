#pragma once

#include "communication/bolt/v1/session.hpp"
#include "communication/bolt/v1/states/state.hpp"

namespace bolt
{

class Error : public State
{
public:
    State *run(Session &session) override;
};

}
