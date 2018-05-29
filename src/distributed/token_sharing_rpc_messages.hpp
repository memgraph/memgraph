#pragma once

#include <memory>
#include <string>

#include "communication/rpc/messages.hpp"
#include "distributed/serialization.hpp"

namespace distributed {

RPC_NO_MEMBER_MESSAGE(TokenTransferReq);
RPC_NO_MEMBER_MESSAGE(TokenTransferRes);

using TokenTransferRpc =
    communication::rpc::RequestResponse<TokenTransferReq, TokenTransferRes>;
}  // namespace distributed
