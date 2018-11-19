/// @file

#pragma once

#include "utils/exceptions.hpp"

namespace raft {

/**
 * Base exception class used for all exceptions that can occur within the
 * Raft protocol.
 */
class RaftException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
};

/**
 * This exception should be thrown when attempting to transition between
 * incompatible states, e.g. from `FOLLOWER` to `LEADER`.
 */
class InvalidTransitionException : public RaftException {
 public:
  using RaftException::RaftException;
  InvalidTransitionException(const std::string &old_mode,
                             const std::string &new_mode)
      : RaftException("Invalid transition from " + old_mode + " to " +
                      new_mode) {}
};

}  // namespace raft
