#pragma once

#include "communication/messaging/distributed.hpp"
#include "communication/rpc/rpc.hpp"
#include "data_structures/concurrent/concurrent_map.hpp"
#include "io/network/network_endpoint.hpp"
#include "storage/concurrent_id_mapper.hpp"

namespace storage {

/** Worker implementation of ConcurrentIdMapper. */
template <typename TId>
class WorkerConcurrentIdMapper : public ConcurrentIdMapper<TId> {
  // Makes an appropriate RPC call for the current TId type and the given value.
  TId RpcValueToId(const std::string &value);

  // Makes an appropriate RPC call for the current TId type and the given value.
  std::string RpcIdToValue(TId id);

 public:
  WorkerConcurrentIdMapper(communication::messaging::System &system,
                           const io::network::NetworkEndpoint &master_endpoint);

  TId value_to_id(const std::string &value) override;
  const std::string &id_to_value(const TId &id) override;

 private:
  // Sources of truth for the mappings are on the master, not on this worker. We
  // keep the caches.
  ConcurrentMap<std::string, TId> value_to_id_cache_;
  ConcurrentMap<TId, std::string> id_to_value_cache_;

  // Communication to the concurrent ID master.
  mutable communication::rpc::Client rpc_client_;
};
}  // namespace storage
