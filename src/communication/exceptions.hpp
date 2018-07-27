#pragma once

#include "utils/exceptions.hpp"

namespace communication {

/**
 * This exception is thrown to indicate to the communication stack that the
 * session is closed and that cleanup should be performed.
 */
class SessionClosedException : public utils::BasicException {
  using utils::BasicException::BasicException;
};
}  // namespace communication
