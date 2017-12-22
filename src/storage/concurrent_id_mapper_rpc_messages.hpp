#pragma once

#include <chrono>

#include "communication/rpc/rpc.hpp"
#include "database/graph_db_datatypes.hpp"
#include "transactions/commit_log.hpp"
#include "transactions/snapshot.hpp"
#include "transactions/type.hpp"
#include "utils/rpc_pimp.hpp"

namespace storage {

const std::string kConcurrentIdMapperRpc = "ConcurrentIdMapper";
const auto kConcurrentIdMapperRpcTimeout = 300ms;

#define ID_VALUE_RPC(type)                                           \
  RPC_SINGLE_MEMBER_MESSAGE(type##IdReq, std::string);               \
  RPC_SINGLE_MEMBER_MESSAGE(type##IdRes, GraphDbTypes::type);        \
  using type##IdRpc =                                                \
      communication::rpc::RequestResponse<type##IdReq, type##IdRes>; \
  RPC_SINGLE_MEMBER_MESSAGE(Id##type##Req, GraphDbTypes::type);      \
  RPC_SINGLE_MEMBER_MESSAGE(Id##type##Res, std::string);             \
  using Id##type##Rpc =                                              \
      communication::rpc::RequestResponse<Id##type##Req, Id##type##Res>;

ID_VALUE_RPC(Label)
ID_VALUE_RPC(EdgeType)
ID_VALUE_RPC(Property)

#undef ID_VALUE_RPC

}  // namespace storage
