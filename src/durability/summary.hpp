#pragma once

namespace snapshot {
/**
 * Struct represents graph summary in a snapshot file.
 */
struct Summary {
  int64_t vertex_num_ = 0LL;
  int64_t edge_num_ = 0LL;
  uint64_t hash_ = 0ULL;
};
}
