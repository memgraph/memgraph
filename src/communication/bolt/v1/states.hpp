#pragma once

#include "communication/bolt/v1/states/state.hpp"
#include "logging/log.hpp"

namespace bolt {

class States {
 public:
  States();

  State::uptr handshake;
  State::uptr init;
  State::uptr executor;
  State::uptr error;
};
}
