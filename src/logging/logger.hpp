#pragma once

#include <fmt/format.h>

#include "logging/levels.hpp"
#include "logging/log.hpp"
#include "utils/assert.hpp"

class Logger {
  template <class Level>
  class Message : public Log::Record {
   public:
    Message(Timestamp timestamp, std::string location, std::string message)
        : timestamp(timestamp), location(location), message(message) {}

    const Timestamp &when() const override { return timestamp; }

    const std::string &where() const override { return location; }

    unsigned level() const override { return Level::level; }

    const std::string &level_str() const override { return Level::text; }

    const std::string &text() const override { return message; }

   private:
    Timestamp timestamp;
    std::string location;
    std::string message;
  };

 public:
  Logger() = default;

  Logger(Log *log, const std::string &name) : log(log), name(name) {}

  template <class Level, class... Args>
  void emit(Args &&... args) {
    debug_assert(log != nullptr, "Log object has to be defined.");

    auto message = std::make_unique<Message<Level>>(
        Timestamp::now(), name, fmt::format(std::forward<Args>(args)...));

    log->emit(std::move(message));
  }

  /**
   *@brief Return if the logger is initialized.
   *@return true if initialized, false otherwise.
   */
  bool Initialized() { return log != nullptr; }

  template <class... Args>
  void trace(Args &&... args) {
#ifndef NDEBUG
#ifndef LOG_NO_TRACE
    emit<Trace>(std::forward<Args>(args)...);
#endif
#endif
  }

  template <class... Args>
  void debug(Args &&... args) {
#ifndef NDEBUG
#ifndef LOG_NO_DEBUG
    emit<Debug>(std::forward<Args>(args)...);
#endif
#endif
  }

  template <class... Args>
  void info(Args &&... args) {
#ifndef LOG_NO_INFO
    emit<Info>(std::forward<Args>(args)...);
#endif
  }

  template <class... Args>
  void warn(Args &&... args) {
#ifndef LOG_NO_WARN
    emit<Warn>(std::forward<Args>(args)...);
#endif
  }

  template <class... Args>
  void error(Args &&... args) {
#ifndef LOG_NO_ERROR
    emit<Error>(std::forward<Args>(args)...);
#endif
  }

 private:
  Log *log;
  std::string name;
};
