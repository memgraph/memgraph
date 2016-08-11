#pragma once

#include <cstdlib>
#include <cstdint>
#include <memory>

namespace bolt
{

class Session;

class State
{
public:
    using uptr = std::unique_ptr<State>;

    State() = default;
    virtual ~State() = default;

    virtual State* run(Session& session) = 0;
};

}
