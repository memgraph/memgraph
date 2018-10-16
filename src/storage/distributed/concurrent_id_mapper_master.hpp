#pragma once

#include <experimental/optional>

#include "distributed/coordination.hpp"
#include "storage/distributed/concurrent_id_mapper_single_node.hpp"

namespace storage {

/** Master implementation of ConcurrentIdMapper. */
template <typename TId>
class MasterConcurrentIdMapper : public SingleNodeConcurrentIdMapper<TId> {
 public:
  explicit MasterConcurrentIdMapper(distributed::Coordination *coordination);

 private:
  distributed::Coordination *coordination_;
};
}  // namespace storage
