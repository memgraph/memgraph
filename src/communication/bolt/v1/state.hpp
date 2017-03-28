#pragma once

namespace communication::bolt {

/**
 * TODO (mferencevic): change to a class enum & document (explain states in
 *                     more details)
 */
enum State {
  HANDSHAKE,
  INIT,
  EXECUTOR,
  ERROR,
  NULLSTATE
};

}
