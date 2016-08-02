#include "states.hpp"

#include "states/handshake.hpp"
#include "states/init.hpp"
#include "states/executor.hpp"

namespace bolt
{

States::States()
{
    handshake = std::make_unique<Handshake>();
    init = std::make_unique<Init>();
    executor = std::make_unique<Executor>();
}

}
