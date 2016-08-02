#pragma once

#include "bolt/v1/states/state.hpp"

namespace bolt
{

class Error : public State
{
public:
    State* run(Session& session) override;
};

}
