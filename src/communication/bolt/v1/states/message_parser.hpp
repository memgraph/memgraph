#pragma once

#include "communication/bolt/v1/session.hpp"
#include "communication/bolt/v1/states/state.hpp"
#include "utils/crtp.hpp"

namespace bolt {

template <class Derived>
class MessageParser : public State, public Crtp<Derived> {
 public:
  MessageParser(Logger &&logger) : logger(std::forward<Logger>(logger)) {}

  State *run(Session &session) override final {
    typename Derived::Message message;

    logger.debug("Parsing message");
    auto next = this->derived().parse(session, message);

    // return next state if parsing was unsuccessful (i.e. error state)
    if (next != &this->derived()) return next;

    logger.debug("Executing state");
    return this->derived().execute(session, message);
  }

 protected:
  Logger logger;
};
}
