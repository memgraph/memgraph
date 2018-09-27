#include "glog/logging.h"

#include "concurrent_id_mapper_worker.hpp"
#include "storage/concurrent_id_mapper_rpc_messages.hpp"
#include "storage/types.hpp"

namespace storage {

#define ID_VALUE_RPC_CALLS(type)                                      \
  template <>                                                         \
  type WorkerConcurrentIdMapper<type>::RpcValueToId(                  \
      const std::string &value) {                                     \
    return master_client_pool_.Call<type##IdRpc>(value).member;       \
  }                                                                   \
                                                                      \
  template <>                                                         \
  std::string WorkerConcurrentIdMapper<type>::RpcIdToValue(type id) { \
    return master_client_pool_.Call<Id##type##Rpc>(id).member;        \
  }

using namespace storage;
ID_VALUE_RPC_CALLS(Label)
ID_VALUE_RPC_CALLS(EdgeType)
ID_VALUE_RPC_CALLS(Property)

#undef ID_VALUE_RPC_CALLS

template <typename TId>
WorkerConcurrentIdMapper<TId>::WorkerConcurrentIdMapper(
    communication::rpc::ClientPool &master_client_pool)
    : master_client_pool_(master_client_pool) {}

template <typename TId>
TId WorkerConcurrentIdMapper<TId>::value_to_id(const std::string &value) {
  auto accessor = value_to_id_cache_.access();
  auto found = accessor.find(value);
  if (found != accessor.end()) return found->second;

  TId id = RpcValueToId(value);
  accessor.insert(value, id);
  return id;
}

template <typename TId>
const std::string &WorkerConcurrentIdMapper<TId>::id_to_value(const TId &id) {
  auto accessor = id_to_value_cache_.access();
  auto found = accessor.find(id);
  if (found != accessor.end()) return found->second;
  std::string value = RpcIdToValue(id);
  return accessor.insert(id, value).first->second;
}

template class WorkerConcurrentIdMapper<Label>;
template class WorkerConcurrentIdMapper<EdgeType>;
template class WorkerConcurrentIdMapper<Property>;
}  // namespace storage
