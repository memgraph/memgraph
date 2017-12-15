#pragma once

#include <experimental/optional>

#include "communication/messaging/distributed.hpp"
#include "communication/rpc/rpc.hpp"
#include "data_structures/concurrent/concurrent_map.hpp"
#include "storage/concurrent_id_mapper_single_node.hpp"

namespace storage {

/** Master implementation of ConcurrentIdMapper. */
template <typename TId>
class MasterConcurrentIdMapper
    : public SingleNodeConcurrentIdMapper<TId, std::string> {

 public:
  MasterConcurrentIdMapper(communication::messaging::System &system);
  ~MasterConcurrentIdMapper();

 private:
  communication::rpc::Server rpc_server_;
};
}  // namespace storage
