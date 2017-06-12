#include <cstdint>

// transcation and command types defined
// in a separate header to avoid cyclic dependencies
namespace tx {

  /** Type of a tx::Transcation's id member */
  using transaction_id_t = uint64_t;

  /** Type of a tx::Transcation's command id member */
  using command_id_t = uint64_t;
}
