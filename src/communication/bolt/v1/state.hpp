#pragma once

namespace communication::bolt {

enum State {
  HANDSHAKE,
  INIT,
  EXECUTOR,
  ERROR,
  NULLSTATE
};

}
