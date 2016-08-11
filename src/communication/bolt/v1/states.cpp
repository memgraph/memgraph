#include "communication/bolt/v1/states.hpp"

#include "communication/bolt/v1/states/handshake.hpp"
#include "communication/bolt/v1/states/init.hpp"
#include "communication/bolt/v1/states/error.hpp"
#include "communication/bolt/v1/states/executor.hpp"

namespace bolt
{

States::States()
{
    handshake = std::make_unique<Handshake>();
    init = std::make_unique<Init>();
    executor = std::make_unique<Executor>();
    error = std::make_unique<Error>();
}

}
