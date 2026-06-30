// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "gtest/gtest.h"

#include <nlohmann/json.hpp>

#include "coordination/coordinator_rpc.hpp"
#include "coordination/instance_state.hpp"
#include "replication_coordination_glue/common.hpp"

#include "rpc_messages.hpp"

#include "auth/rpc.hpp"
#include "replication_handler/system_rpc.hpp"
#include "rpc/client.hpp"
#include "rpc/file_replication_handler.hpp"
#include "rpc/server.hpp"
#include "rpc/utils.hpp"  // Needs to be included last so that SLK definitions are seen
#include "slk/streams.hpp"

using memgraph::communication::ClientContext;
using memgraph::communication::ServerContext;

using memgraph::io::network::Endpoint;
using memgraph::rpc::Client;
using memgraph::rpc::Server;
using memgraph::storage::replication::HeartbeatRpc;

using namespace std::string_view_literals;
using namespace std::literals::chrono_literals;

namespace {
constexpr int port{8182};
}  // namespace

// RPC client is setup with timeout but shouldn't be triggered.
TEST(RpcVersioning, SumUpgrade) {
  Endpoint const endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<Sum>([](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                              uint64_t const request_version,
                              auto *req_reader,
                              auto *res_builder) {
    SumReq req;
    memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);

    auto const sum = std::accumulate(req.nums_.begin(), req.nums_.end(), 0);

    SumRes const res({sum});
    memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
  });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  auto const rpc_timeouts = std::unordered_map{std::make_pair("EchoMessage"sv, 2000)};
  ClientContext client_context;
  Client client{endpoint, &client_context, rpc_timeouts};
  {
    // Send new version request
    auto stream = client.Stream<Sum>(std::initializer_list<int>{35, 30});
    auto reply = stream.SendAndWait();
    EXPECT_EQ(reply.sum, std::vector<int>{65});
  }
  {
    // Send old versioned request
    auto stream = client.Stream<SumV1>(10, 12);
    auto reply = stream.SendAndWait();
    EXPECT_EQ(reply.sum, 22);
  }
}

#ifdef MG_ENTERPRISE

namespace memgraph::coordination {
using GetDatabaseHistoriesRpcV1 = rpc::RequestResponse<GetDatabaseHistoriesReqV1, GetDatabaseHistoriesResV1>;
}  // namespace memgraph::coordination

TEST(RpcVersioning, GetDBHistories) {
  Endpoint const endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<memgraph::coordination::GetDatabaseHistoriesRpc>(
      [](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
         uint64_t const request_version,
         auto * /*req_reader*/,
         auto *res_builder) {
        // The request is empty hence I don't need to call LoadWithUpgrade

        if (request_version == memgraph::coordination::GetDatabaseHistoriesReqV1::kVersion) {
          memgraph::coordination::GetDatabaseHistoriesResV1 res;
          res.arg_.last_committed_system_timestamp = 81;
          res.arg_.dbs_info = std::vector{memgraph::replication_coordination_glue::InstanceDBInfoV1{
                                              .db_uuid = "123", .latest_durable_timestamp = 4},
                                          memgraph::replication_coordination_glue::InstanceDBInfoV1{
                                              .db_uuid = "1234", .latest_durable_timestamp = 13}};

          memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
        } else {
          memgraph::coordination::GetDatabaseHistoriesRes res;
          res.arg_.last_committed_system_timestamp = 81;
          res.arg_.dbs_info = std::vector{
              memgraph::replication_coordination_glue::InstanceDBInfo{.db_uuid = "123", .num_committed_txns = 2},
              memgraph::replication_coordination_glue::InstanceDBInfo{.db_uuid = "1234", .num_committed_txns = 22}};

          memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
        }
      });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  ClientContext client_context;
  Client client{endpoint, &client_context};
  {
    // Send new version request
    auto stream = client.Stream<memgraph::coordination::GetDatabaseHistoriesRpc>();

    auto reply = stream.SendAndWait();

    EXPECT_EQ(reply.arg_.last_committed_system_timestamp, 81);
    auto const dbs_info_res = std::vector{
        {memgraph::replication_coordination_glue::InstanceDBInfo{.db_uuid = "123", .num_committed_txns = 2},
         memgraph::replication_coordination_glue::InstanceDBInfo{.db_uuid = "1234", .num_committed_txns = 22}}};
    EXPECT_EQ(reply.arg_.dbs_info, dbs_info_res);
  }

  {
    // Send old version request
    auto stream = client.Stream<memgraph::coordination::GetDatabaseHistoriesRpcV1>();
    auto reply = stream.SendAndWait();
    EXPECT_EQ(reply.arg_.last_committed_system_timestamp, 81);
    auto const dbs_info_res = std::vector{
        {memgraph::replication_coordination_glue::InstanceDBInfoV1{.db_uuid = "123", .latest_durable_timestamp = 4},
         memgraph::replication_coordination_glue::InstanceDBInfoV1{
             .db_uuid = "1234",
             .latest_durable_timestamp = 13,
         }}};
    EXPECT_EQ(reply.arg_.dbs_info, dbs_info_res);
  }
}

namespace memgraph::coordination {
using StateCheckRpcV1 = rpc::RequestResponse<StateCheckReqV1, StateCheckResV1>;
}  // namespace memgraph::coordination

TEST(RpcVersioning, StateCheckRpc) {
  Endpoint const endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  std::map<std::string, uint64_t> const main_num_txns{{"a", 1}, {"b", 5}, {"c", 9}};
  std::map<std::string, std::map<std::string, int64_t>> const replicas_num_txns{
      {"instance_1", std::map<std::string, int64_t>{{"a", 1}, {"b", 4}, {"c", 6}}},
      {"instance_2", std::map<std::string, int64_t>{{"a", 1}, {"b", 6}, {"c", 9}}}

  };

  rpc_server.Register<memgraph::coordination::StateCheckRpc>(
      [&main_num_txns, &replicas_num_txns](
          std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          auto * /*req_reader*/,
          auto *res_builder) {
        memgraph::coordination::InstanceStateV2 inner_state{.is_replica = false,
                                                            .uuid = memgraph::utils::UUID{},
                                                            .is_writing_enabled = true,
                                                            .main_num_txns = main_num_txns,
                                                            .replicas_num_txns = replicas_num_txns};
        memgraph::coordination::InstanceState const instance_state{.inner_state = std::move(inner_state),
                                                                   .deltas_batch_progress_size = 12000};
        memgraph::coordination::StateCheckRes const res{instance_state};
        memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
      });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  ClientContext client_context;
  Client client{endpoint, &client_context};
  {
    // Send new version request
    auto stream = client.Stream<memgraph::coordination::StateCheckRpc>();
    auto reply = stream.SendAndWait();

    EXPECT_FALSE(reply.arg_.inner_state.is_replica);
    EXPECT_TRUE(reply.arg_.inner_state.is_writing_enabled);
    EXPECT_EQ(*reply.arg_.inner_state.main_num_txns, main_num_txns);
    EXPECT_EQ(*reply.arg_.inner_state.replicas_num_txns, replicas_num_txns);
  }

  {
    // Send old version request
    auto stream = client.Stream<memgraph::coordination::StateCheckRpcV1>();
    auto reply = stream.SendAndWait();
    EXPECT_FALSE(reply.arg_.is_replica);
    EXPECT_TRUE(reply.arg_.is_writing_enabled);
  }
}
#endif

// Test: when request has 2 versions but response has only one version (no Downgrade),
// SendFinalResponse(res, request_version=1, ...) throws because SaveWithDowngrade cannot produce v1.
TEST(RpcVersioning, RequestTwoVersionsSingleVersionResponse_ThrowsWhenSendingV1) {
  std::vector<uint8_t> sink;
  memgraph::slk::Builder builder(
      [&sink](const uint8_t *data, size_t size, bool) { sink.insert(sink.end(), data, data + size); });
  TestResSingleVersion res;
  EXPECT_THROW(memgraph::rpc::SaveWithDowngrade(res, 1, &builder), std::runtime_error);
}

// SystemRecoveryRpc: with ResV1 and Downgrade, both v1 and v2 requests get correct response.
TEST(RpcVersioning, SystemRecoveryRpc_V1AndV2Request_BothSucceed) {
  Endpoint const endpoint{"localhost", port};

  ServerContext server_context;
  Server rpc_server{endpoint, &server_context, /* workers */ 1};
  auto const on_exit = memgraph::utils::OnScopeExit{[&rpc_server] {
    ASSERT_TRUE(rpc_server.Shutdown());
    rpc_server.AwaitShutdown();
  }};

  rpc_server.Register<memgraph::replication::SystemRecoveryRpc>(
      [](std::optional<memgraph::rpc::FileReplicationHandler> const & /*file_replication_handler*/,
         uint64_t const request_version,
         auto *req_reader,
         auto *res_builder) {
        memgraph::replication::SystemRecoveryReq req;
        memgraph::rpc::LoadWithUpgrade(req, request_version, req_reader);
        memgraph::replication::SystemRecoveryRes const res(memgraph::replication::SystemRecoveryRes::Result::SUCCESS);
        memgraph::rpc::SendFinalResponse(res, request_version, res_builder);
      });

  ASSERT_TRUE(rpc_server.Start());
  std::this_thread::sleep_for(100ms);

  ClientContext client_context;
  Client client{endpoint, &client_context};
  {
    auto stream =
        client.Stream<memgraph::replication::SystemRecoveryRpc>(memgraph::utils::UUID{},
                                                                0,
                                                                std::vector<memgraph::storage::SalientConfig>{},
                                                                memgraph::auth::Auth::Config{},
                                                                std::vector<memgraph::auth::User>{},
                                                                std::vector<memgraph::auth::Role>{},
                                                                std::vector<memgraph::auth::UserProfiles::Profile>{});
    auto reply = stream.SendAndWait();
    EXPECT_EQ(reply.result, memgraph::replication::SystemRecoveryRes::Result::SUCCESS);
  }
  {
    auto stream =
        client.Stream<memgraph::replication::SystemRecoveryRpc>(memgraph::utils::UUID{},
                                                                0,
                                                                std::vector<memgraph::storage::SalientConfig>{},
                                                                memgraph::auth::Auth::Config{},
                                                                std::vector<memgraph::auth::User>{},
                                                                std::vector<memgraph::auth::Role>{},
                                                                std::vector<memgraph::auth::UserProfiles::Profile>{},
                                                                std::vector<memgraph::parameters::ParameterInfo>{});
    auto reply = stream.SendAndWait();
    EXPECT_EQ(reply.result, memgraph::replication::SystemRecoveryRes::Result::SUCCESS);
  }
}

#ifdef MG_ENTERPRISE

namespace {
// Build a V3-format role JSON (uses global_permission instead of global_grants/global_denies).
nlohmann::json MakeV3RoleJson(std::string const &rolename, int64_t label_perm, int64_t edge_perm) {
  nlohmann::json data;
  data["version"] = 3;
  data["rolename"] = rolename;
  data["builtin"] = false;
  data["permissions"] = nlohmann::json{{"grants", 0}, {"denies", 0}};
  data["fine_grained_permissions"] = {
      {"label_permissions", {{"global_permission", label_perm}, {"permissions", nlohmann::json::array()}}},
      {"edge_type_permissions", {{"global_permission", edge_perm}, {"permissions", nlohmann::json::array()}}}};
  data["databases"] = {{"allow_all", true},
                       {"grants", nlohmann::json::array()},
                       {"denies", nlohmann::json::array()},
                       {"default", "memgraph"}};
  return data;
}

// Build a V3-format user JSON.
nlohmann::json MakeV3UserJson(std::string const &username, int64_t label_perm, int64_t edge_perm) {
  nlohmann::json data;
  data["version"] = 3;
  data["username"] = username;
  data["uuid"] = memgraph::utils::UUID{};
  data["password_hash"] = nullptr;
  data["permissions"] = nlohmann::json{{"grants", 0}, {"denies", 0}};
  data["fine_grained_permissions"] = {
      {"label_permissions", {{"global_permission", label_perm}, {"permissions", nlohmann::json::array()}}},
      {"edge_type_permissions", {{"global_permission", edge_perm}, {"permissions", nlohmann::json::array()}}}};
  data["databases"] = {{"allow_all", true},
                       {"grants", nlohmann::json::array()},
                       {"denies", nlohmann::json::array()},
                       {"default", "memgraph"}};
  return data;
}

// SLK round-trip helper: serialize V1, then LoadWithUpgrade to V2.
memgraph::replication::UpdateAuthDataReq RoundTripV1ToV2(memgraph::replication::UpdateAuthDataReqV1 const &v1_req) {
  std::vector<uint8_t> buf;
  memgraph::slk::Builder builder(
      [&buf](const uint8_t *data, size_t size, bool) { buf.insert(buf.end(), data, data + size); });
  memgraph::slk::Save(v1_req, &builder);
  builder.Finalize();

  memgraph::slk::Reader reader(buf.data(), buf.size());
  memgraph::replication::UpdateAuthDataReq v2_req;
  memgraph::rpc::LoadWithUpgrade(v2_req, /*request_version=*/1, &reader);
  return v2_req;
}
}  // namespace

// V1 request with V3-format FGA User JSON → upgraded to V2 with migrated FGA.
TEST(RpcVersioning, UpdateAuthDataRpc_V1UserWithOldFGA_MigratedOnUpgrade) {
  // V3 global_permission = 1 means READ granted.
  // After V3→V4 migration: global_grants=1 (READ), global_denies=-1 (unset).
  auto user_json = MakeV3UserJson("alice", /*label_perm=*/1, /*edge_perm=*/1);
  auto role_json = MakeV3RoleJson("testrole", /*label_perm=*/1, /*edge_perm=*/1);

  memgraph::replication::UpdateAuthDataReqV1 v1;
  v1.main_uuid = memgraph::utils::UUID{};
  v1.expected_group_timestamp = 42;
  v1.new_group_timestamp = 43;
  v1.user_json = user_json.dump();
  v1.user_role_jsons = std::vector<std::string>{role_json.dump()};
  v1.user_mt_mappings = std::unordered_map<std::string, std::unordered_set<std::string>>{};

  auto v2 = RoundTripV1ToV2(v1);

  ASSERT_TRUE(v2.user.has_value());
  EXPECT_EQ(v2.user->username(), "alice");
  EXPECT_FALSE(v2.role.has_value());
  EXPECT_EQ(v2.expected_group_timestamp, 42);
  EXPECT_EQ(v2.new_group_timestamp, 43);

  // Verify FGA migrated: global_grants should be set (not nullopt),
  // and the old global_permission field should be gone.
  auto const &label_perms = v2.user->fine_grained_access_handler().label_permissions();
  ASSERT_TRUE(label_perms.GetGlobalGrants().has_value());
  // V3 perm=1 (READ) → V4 global_grants=1 (READ bit), but label perms also
  // expand UPDATE→multiple bits. READ=1 stays as 1.
  EXPECT_EQ(label_perms.GetGlobalGrants().value(), static_cast<uint64_t>(memgraph::auth::FineGrainedPermission::READ));

  // Check the embedded role was also migrated
  auto const &roles = v2.user->roles();
  ASSERT_EQ(roles.size(), 1);
  auto const &role = *roles.begin();
  EXPECT_EQ(role.rolename(), "testrole");
  auto const &role_label_perms = role.fine_grained_access_handler().label_permissions();
  ASSERT_TRUE(role_label_perms.GetGlobalGrants().has_value());
  EXPECT_EQ(role_label_perms.GetGlobalGrants().value(), 1);
}

// V1 request with V3-format FGA standalone Role → upgraded with migration.
TEST(RpcVersioning, UpdateAuthDataRpc_V1RoleWithOldFGA_MigratedOnUpgrade) {
  auto role_json = MakeV3RoleJson("myrole", /*label_perm=*/1, /*edge_perm=*/1);

  memgraph::replication::UpdateAuthDataReqV1 v1;
  v1.main_uuid = memgraph::utils::UUID{};
  v1.expected_group_timestamp = 10;
  v1.new_group_timestamp = 11;
  v1.role_json = role_json.dump();

  auto v2 = RoundTripV1ToV2(v1);

  ASSERT_FALSE(v2.user.has_value());
  ASSERT_TRUE(v2.role.has_value());
  EXPECT_EQ(v2.role->rolename(), "myrole");

  auto const &label_perms = v2.role->fine_grained_access_handler().label_permissions();
  ASSERT_TRUE(label_perms.GetGlobalGrants().has_value());
  EXPECT_EQ(label_perms.GetGlobalGrants().value(), static_cast<uint64_t>(memgraph::auth::FineGrainedPermission::READ));
}

// V2 request round-trips without migration.
TEST(RpcVersioning, UpdateAuthDataRpc_V2Request_NoMigrationNeeded) {
  auto role = memgraph::auth::Role("v2role");
  role.fine_grained_access_handler().label_permissions().GrantGlobal(memgraph::auth::FineGrainedPermission::READ);

  memgraph::replication::UpdateAuthDataReq orig;
  orig.main_uuid = memgraph::utils::UUID{};
  orig.expected_group_timestamp = 5;
  orig.new_group_timestamp = 6;
  orig.role = std::move(role);

  // Serialize as V2 and load as V2 (no upgrade needed).
  std::vector<uint8_t> buf;
  memgraph::slk::Builder builder(
      [&buf](const uint8_t *data, size_t size, bool) { buf.insert(buf.end(), data, data + size); });
  memgraph::slk::Save(orig, &builder);
  builder.Finalize();

  memgraph::slk::Reader reader(buf.data(), buf.size());
  memgraph::replication::UpdateAuthDataReq loaded;
  memgraph::rpc::LoadWithUpgrade(loaded, /*request_version=*/2, &reader);

  ASSERT_TRUE(loaded.role.has_value());
  EXPECT_EQ(loaded.role->rolename(), "v2role");
  auto const &label_perms = loaded.role->fine_grained_access_handler().label_permissions();
  ASSERT_TRUE(label_perms.GetGlobalGrants().has_value());
  EXPECT_EQ(label_perms.GetGlobalGrants().value(), static_cast<uint64_t>(memgraph::auth::FineGrainedPermission::READ));
}

// V1 request with user but no roles (user_role_jsons = nullopt).
TEST(RpcVersioning, UpdateAuthDataRpc_V1UserNoRoles_UpgradesCleanly) {
  auto user_json = MakeV3UserJson("bob", /*label_perm=*/1, /*edge_perm=*/1);

  memgraph::replication::UpdateAuthDataReqV1 v1;
  v1.main_uuid = memgraph::utils::UUID{};
  v1.expected_group_timestamp = 1;
  v1.new_group_timestamp = 2;
  v1.user_json = user_json.dump();
  // user_role_jsons and user_mt_mappings left as nullopt

  auto v2 = RoundTripV1ToV2(v1);

  ASSERT_TRUE(v2.user.has_value());
  EXPECT_EQ(v2.user->username(), "bob");
  EXPECT_TRUE(v2.user->roles().empty());

  auto const &label_perms = v2.user->fine_grained_access_handler().label_permissions();
  ASSERT_TRUE(label_perms.GetGlobalGrants().has_value());
  EXPECT_EQ(label_perms.GetGlobalGrants().value(), static_cast<uint64_t>(memgraph::auth::FineGrainedPermission::READ));
}

// V1 request with user and MT role mappings.
TEST(RpcVersioning, UpdateAuthDataRpc_V1UserWithMtMappings_RolesAttachedPerDb) {
  auto user_json = MakeV3UserJson("carol", /*label_perm=*/-1, /*edge_perm=*/-1);
  auto role_json = MakeV3RoleJson("dbrole", /*label_perm=*/1, /*edge_perm=*/1);

  memgraph::replication::UpdateAuthDataReqV1 v1;
  v1.main_uuid = memgraph::utils::UUID{};
  v1.expected_group_timestamp = 100;
  v1.new_group_timestamp = 101;
  v1.user_json = user_json.dump();
  v1.user_role_jsons = std::vector<std::string>{role_json.dump()};
  v1.user_mt_mappings = std::unordered_map<std::string, std::unordered_set<std::string>>{{"testdb", {"dbrole"}}};

  auto v2 = RoundTripV1ToV2(v1);

  ASSERT_TRUE(v2.user.has_value());
  EXPECT_EQ(v2.user->username(), "carol");

  // The role should be attached both as a regular role and as an MT mapping
  EXPECT_EQ(v2.user->roles().size(), 1);
  EXPECT_EQ(v2.user->roles().begin()->rolename(), "dbrole");

  auto const &mt = v2.user->GetMultiTenantRoleMappings();
  ASSERT_EQ(mt.count("testdb"), 1);
  EXPECT_EQ(mt.at("testdb").count("dbrole"), 1);
}

#endif  // MG_ENTERPRISE
