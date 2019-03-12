/// @file

#pragma once

#include <memory>

namespace distributed {
/// A wrapper for cached vertex/edge from other machines in the distributed
/// system.
///
/// @tparam TRecord Vertex or Edge
template <typename TRecord>
struct CachedRecordData {
  CachedRecordData(int64_t cypher_id, std::unique_ptr<TRecord> old_record,
                   std::unique_ptr<TRecord> new_record)
      : cypher_id(cypher_id),
        old_record(std::move(old_record)),
        new_record(std::move(new_record)) {}
  int64_t cypher_id;
  std::unique_ptr<TRecord> old_record;
  std::unique_ptr<TRecord> new_record;
};
} // namespace distributed
