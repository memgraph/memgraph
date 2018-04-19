#pragma once

#include "communication/rpc/messages.hpp"
#include "transactions/type.hpp"

namespace distributed {

RPC_SINGLE_MEMBER_MESSAGE(WaitOnTransactionEndReq, tx::TransactionId);
RPC_NO_MEMBER_MESSAGE(WaitOnTransactionEndRes);
using WaitOnTransactionEndRpc =
    communication::rpc::RequestResponse<WaitOnTransactionEndReq,
                                        WaitOnTransactionEndRes>;
};
