// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <iostream>

#include <coordination/replication_instance_client.hpp>
#include <replication_coordination_glue/handler.hpp>
#include <replication_coordination_glue/mode.hpp>

import memgraph.coordination.coordinator_rpc;

bool SendSwapUUID(const char *address, int port, const char *uuid) {
  try {
    memgraph::communication::ClientContext cntxt{};
    memgraph::io::network::Endpoint endpoint{address, static_cast<uint16_t>(port)};
    memgraph::rpc::Client rpc_client{endpoint, &cntxt};
    memgraph::utils::UUID new_uuid{};
    new_uuid.set(uuid);
    std::cout << "Sending swap UUID " << std::string(new_uuid) << " to " << endpoint.SocketAddress() << std::endl;
    return memgraph::replication_coordination_glue::SendSwapMainUUIDRpc(rpc_client, new_uuid);
  } catch (...) {
    return false;
  }
}

bool SendPromoteToMain(const char *address, int port, const char *uuid) {
  try {
    memgraph::communication::ClientContext cntxt{};
    memgraph::io::network::Endpoint endpoint{address, static_cast<uint16_t>(port)};
    memgraph::rpc::Client rpc_client{endpoint, &cntxt};
    memgraph::utils::UUID new_uuid{};
    new_uuid.set(uuid);
    // Hard code the cluster definition... change if needed
    memgraph::coordination::ReplicationClientsInfo replication_clients_info;
    replication_clients_info.push_back(memgraph::coordination::ReplicationClientInfo{
        .instance_name = "instance_1",
        .replication_mode = memgraph::replication_coordination_glue::ReplicationMode::SYNC,
        .replication_server = {"127.0.0.1", 10001}});
    replication_clients_info.push_back(memgraph::coordination::ReplicationClientInfo{
        .instance_name = "instance_2",
        .replication_mode = memgraph::replication_coordination_glue::ReplicationMode::SYNC,
        .replication_server = {"127.0.0.1", 10002}});
    auto stream{
        rpc_client.Stream<memgraph::coordination::PromoteToMainRpc>(new_uuid, std::move(replication_clients_info))};
    return stream.SendAndWait().success;
  } catch (...) {
    return false;
  }
}

int main(int argc, char **argv) {
  if (!SendPromoteToMain(argv[1], std::stoi(argv[2]), argv[3])) {
    return 1;
  }
  return 0;
}
