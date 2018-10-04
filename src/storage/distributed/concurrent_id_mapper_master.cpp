#include "storage/distributed/concurrent_id_mapper_master.hpp"

#include <glog/logging.h>

#include "storage/common/types.hpp"
#include "storage/distributed/concurrent_id_mapper_rpc_messages.hpp"

namespace storage {

namespace {
template <typename TId>
void RegisterRpc(MasterConcurrentIdMapper<TId> &mapper,
                 communication::rpc::Server &rpc_server);
#define ID_VALUE_RPC_CALLS(type)                                    \
  template <>                                                       \
  void RegisterRpc<type>(MasterConcurrentIdMapper<type> & mapper,   \
                         communication::rpc::Server & rpc_server) { \
    rpc_server.Register<type##IdRpc>(                               \
        [&mapper](const auto &req_reader, auto *res_builder) {      \
          type##IdReq req;                                          \
          Load(&req, req_reader);                                   \
          type##IdRes res(mapper.value_to_id(req.member));          \
          Save(res, res_builder);                                   \
        });                                                         \
    rpc_server.Register<Id##type##Rpc>(                             \
        [&mapper](const auto &req_reader, auto *res_builder) {      \
          Id##type##Req req;                                        \
          Load(&req, req_reader);                                   \
          Id##type##Res res(mapper.id_to_value(req.member));        \
          Save(res, res_builder);                                   \
        });                                                         \
  }

using namespace storage;
ID_VALUE_RPC_CALLS(Label)
ID_VALUE_RPC_CALLS(EdgeType)
ID_VALUE_RPC_CALLS(Property)
#undef ID_VALUE_RPC
}  // namespace

template <typename TId>
MasterConcurrentIdMapper<TId>::MasterConcurrentIdMapper(
    communication::rpc::Server &server)
    // We have to make sure our rpc server name is unique with regards to type.
    // Otherwise we will try to reuse the same rpc server name for different
    // types (Label/EdgeType/Property)
    : rpc_server_(server) {
  RegisterRpc(*this, rpc_server_);
}

template class MasterConcurrentIdMapper<Label>;
template class MasterConcurrentIdMapper<EdgeType>;
template class MasterConcurrentIdMapper<Property>;

}  // namespace storage
