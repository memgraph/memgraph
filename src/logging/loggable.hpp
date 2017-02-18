#pragma once

#include "logging/default.hpp"

/**
 * @class Loggable
 *
 * @brief Base class that could be used in all classed which need a logging
 * functionality.
 */
class Loggable {
 public:
  /**
   * Sets logger name.
   */
  Loggable(const std::string &name) : logger(logging::log->logger(name)) {}

  virtual ~Loggable() {}

 protected:
  /**
   * Logger instance that can be used only from derived classes.
   */
  Logger logger;
};
