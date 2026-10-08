// Copyright 2026 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

#include "auth/replication_action.hpp"

#include "auth/rpc.hpp"
#include "system/transaction.hpp"

#ifdef MG_ENTERPRISE
namespace memgraph::auth {

bool BatchedAuthAction::DoReplication(replication::ReplicationClient &client, const utils::UUID &main_uuid,
                                      memgraph::system::Transaction const &txn) const {
  auto check_response = [](const replication::UpdateAuthDataRes &response) { return response.success; };
  return client.StreamAndFinalizeDelta<replication::UpdateAuthDataRpc>(
      check_response, main_uuid, txn.last_committed_system_timestamp(), txn.timestamp(), ops_);
}

}  // namespace memgraph::auth
#endif
