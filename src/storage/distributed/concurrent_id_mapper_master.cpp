#include "storage/distributed/concurrent_id_mapper_master.hpp"

#include <glog/logging.h>

#include "storage/common/types/types.hpp"
#include "storage/distributed/rpc/concurrent_id_mapper_rpc_messages.hpp"

namespace storage {

namespace {
template <typename TId>
void RegisterRpc(MasterConcurrentIdMapper<TId> *mapper,
                 distributed::Coordination *coordination);
#define ID_VALUE_RPC_CALLS(type)                                     \
  template <>                                                        \
  void RegisterRpc<type>(MasterConcurrentIdMapper<type> * mapper,    \
                         distributed::Coordination * coordination) { \
    coordination->Register<type##IdRpc>(                             \
        [mapper](auto *req_reader, auto *res_builder) {              \
          type##IdReq req;                                           \
          slk::Load(&req, req_reader);                               \
          type##IdRes res(mapper->value_to_id(req.member));          \
          slk::Save(res, res_builder);                               \
        });                                                          \
    coordination->Register<Id##type##Rpc>(                           \
        [mapper](auto *req_reader, auto *res_builder) {              \
          Id##type##Req req;                                         \
          slk::Load(&req, req_reader);                               \
          Id##type##Res res(mapper->id_to_value(req.member));        \
          slk::Save(res, res_builder);                               \
        });                                                          \
  }

using namespace storage;
ID_VALUE_RPC_CALLS(Label)
ID_VALUE_RPC_CALLS(EdgeType)
ID_VALUE_RPC_CALLS(Property)
#undef ID_VALUE_RPC
}  // namespace

template <typename TId>
MasterConcurrentIdMapper<TId>::MasterConcurrentIdMapper(
    distributed::Coordination *coordination)
    // We have to make sure our rpc server name is unique with regards to type.
    // Otherwise we will try to reuse the same rpc server name for different
    // types (Label/EdgeType/Property)
    : coordination_(coordination) {
  RegisterRpc(this, coordination_);
}

template class MasterConcurrentIdMapper<Label>;
template class MasterConcurrentIdMapper<EdgeType>;
template class MasterConcurrentIdMapper<Property>;

}  // namespace storage
