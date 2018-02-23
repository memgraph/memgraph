#pragma once

#include <chrono>

#include "communication/rpc/messages.hpp"
#include "storage/types.hpp"
#include "transactions/commit_log.hpp"
#include "transactions/snapshot.hpp"
#include "transactions/type.hpp"

namespace storage {

#define ID_VALUE_RPC(type)                                           \
  RPC_SINGLE_MEMBER_MESSAGE(type##IdReq, std::string);               \
  RPC_SINGLE_MEMBER_MESSAGE(type##IdRes, storage::type);             \
  using type##IdRpc =                                                \
      communication::rpc::RequestResponse<type##IdReq, type##IdRes>; \
  RPC_SINGLE_MEMBER_MESSAGE(Id##type##Req, storage::type);           \
  RPC_SINGLE_MEMBER_MESSAGE(Id##type##Res, std::string);             \
  using Id##type##Rpc =                                              \
      communication::rpc::RequestResponse<Id##type##Req, Id##type##Res>;

ID_VALUE_RPC(Label)
ID_VALUE_RPC(EdgeType)
ID_VALUE_RPC(Property)

#undef ID_VALUE_RPC

}  // namespace storage
