/// @file

#include <cstdint>

// transcation and command types defined
// in a separate header to avoid cyclic dependencies
namespace tx {

  /// Type of a tx::Transcation's id member
  using TransactionId = uint64_t;

  /// Type of a tx::Transcation's command id member
  using CommandId = uint32_t;
}
