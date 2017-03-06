#pragma once

namespace bolt {

enum State {
  HANDSHAKE,
  INIT,
  EXECUTOR,
  ERROR,
  NULLSTATE
};

}
