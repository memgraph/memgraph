#pragma once

#include "utils/exceptions.hpp"

namespace storage::durability {

/// Exception used to handle errors during recovery.
class RecoveryFailure : public utils::BasicException {
  using utils::BasicException::BasicException;
};

}  // namespace storage::durability
