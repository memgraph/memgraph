#include "glog/logging.h"

#include "concurrent_id_mapper_worker.hpp"
#include "storage/concurrent_id_mapper_rpc_messages.hpp"
#include "storage/types.hpp"

namespace storage {

#define ID_VALUE_RPC_CALLS(type)                                         \
  template <>                                                            \
  type WorkerConcurrentIdMapper<type>::RpcValueToId(                     \
      const std::string &value) {                                        \
    auto response = rpc_client_pool_.Call<type##IdRpc>(value);           \
    CHECK(response) << ("Failed to obtain " #type " from master");       \
    return response->member;                                             \
  }                                                                      \
                                                                         \
  template <>                                                            \
  std::string WorkerConcurrentIdMapper<type>::RpcIdToValue(type id) {    \
    auto response = rpc_client_pool_.Call<Id##type##Rpc>(id);            \
    CHECK(response) << ("Failed to obtain " #type " value from master"); \
    return response->member;                                             \
  }

using namespace storage;
ID_VALUE_RPC_CALLS(Label)
ID_VALUE_RPC_CALLS(EdgeType)
ID_VALUE_RPC_CALLS(Property)

#undef ID_VALUE_RPC_CALLS

template <typename TId>
WorkerConcurrentIdMapper<TId>::WorkerConcurrentIdMapper(
    const io::network::Endpoint &master_endpoint)
    : rpc_client_pool_(master_endpoint, impl::RpcServerNameFromType<TId>()) {}

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
