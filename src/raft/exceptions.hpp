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

/**
 * Exception used to indicate something is wrong with the raft config provided
 * by the user.
 */
class RaftConfigException : public RaftException {
 public:
  using RaftException::RaftException;
  explicit RaftConfigException(const std::string &path)
      : RaftException("Unable to parse raft config file " + path) {}
};

/**
 * Exception used to indicate something is wrong with the coordination config
 * provided by the user.
 */
class RaftCoordinationConfigException : public RaftException {
 public:
  using RaftException::RaftException;
  explicit RaftCoordinationConfigException(const std::string &path)
      : RaftException("Unable to parse raft coordination config file " + path) {
  }
};

}  // namespace raft
