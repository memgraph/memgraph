#pragma once

#include <string>

namespace storage {

/**
 * Defines an interface for mapping IDs to values and vice-versa. The interface
 * is necessary because in the distributed system implementations are different
 * for the master (single source of truth) and worker (must query master).
 * Both implementations must be concurrent.
 *
 * @TParam TId - One of GraphDb types (Label, EdgeType, Property).
 */
template <typename TId>
class ConcurrentIdMapper {
 public:
  virtual TId value_to_id(const std::string &value) = 0;
  virtual const std::string &id_to_value(const TId &id) = 0;
  virtual ~ConcurrentIdMapper() {}
};
}  // namespace storage
