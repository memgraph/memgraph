#pragma once

#include <cstdint>

namespace communication::bolt {

/**
 * This class represents states in execution of the Bolt protocol.
 * It is used only internally in the Session. All functions that run
 * these states can be found in the states/ subdirectory.
 */
enum class State : uint8_t {
  /**
   * This state negotiates a handshake with the client.
   */
  Handshake,

  /**
   * This state initializes the Bolt session.
   */
  Init,

  /**
   * This state waits for next query (RUN command).
   */
  Idle,

  /**
   * This state holds results of RUN command and waits for either PULL_ALL or
   * DISCARD_ALL command.
   */
  Result,

  /**
   * This state handles errors.
   */
  Error,

  /**
   * This is a 'virtual' state (it doesn't have a run function) which tells
   * the session that the client has sent malformed data and that the
   * session should be closed.
   */
  Close
};
}
