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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <filesystem>
#include <type_traits>
#include <variant>

#include "auth/crypto.hpp"
#include "auth/exceptions.hpp"
#include "auth/models.hpp"
#include "kvstore/kvstore.hpp"
#include "license/license.hpp"
#include "nlohmann/json.hpp"

namespace {
constexpr auto kRoleName = "rolename";
constexpr auto kPermissions = "permissions";
constexpr auto kGrants = "grants";
constexpr auto kDenies = "denies";
constexpr auto kUsername = "username";
constexpr auto kUUID = "uuid";
constexpr auto kPasswordHash = "password_hash";
constexpr auto kLabelPermissions = "label_permissions";
constexpr auto kEdgeTypePermissions = "edge_type_permissions";
constexpr auto kHashAlgo = "hash_algo";

constexpr auto kGlobalPermission = "global_permission";
constexpr auto kFineGrainedAccessHandler = "fine_grained_access_handler";
constexpr auto kAllowAll = "allow_all";
constexpr auto kDefault = "default";
constexpr auto kDatabases = "databases";
constexpr auto kUserImp = "user_imp";
constexpr auto kUserImpGranted = "user_imp_granted";
constexpr auto kUserImpDenied = "user_imp_denied";
constexpr auto kUserImpId = "user_imp_id";
constexpr auto kUserImpName = "user_imp_name";

auto User2Role(auto json) {
  json[kRoleName] = json[kUsername];
  json.erase(kUsername);
  json.erase(kUUID);
  json.erase(kPasswordHash);
  return json;
}

constexpr auto full_json_str = R"({
  "databases":{"allow_all":true,"default":"db1","denies":["db2","db3"],"grants":["db1", "memgraph"]},
  "fine_grained_access_handler":{
    "edge_type_permissions":{"global_permission":7,"permissions":{"E":1}},
    "label_permissions":{"global_permission":7,"permissions":{"A":1,"B":1,"C":3,"D":0}}
  },
  "password_hash":{"hash_algo":0,"password_hash":"$2a$12$pFMD3q0mfCg.lPD3ng0F5uzOCi5n4VZTDklBc2lQyXi19AaUwJXAa"},
  "permissions":{"denies":0,"grants":134217727},
  "user_imp":{
    "user_imp_denied":[
      {"user_imp_id":[90,135,181,89,151,124,74,168,175,106,33,91,219,3,104,168],"user_imp_name":"b"}
    ],
    "user_imp_granted":[
      {"user_imp_id":[12,135,181,89,151,124,74,168,175,106,33,91,219,3,104,168],"user_imp_name":"c"},
      {"user_imp_id":[23,135,181,89,151,124,74,168,175,106,33,91,219,3,104,168],"user_imp_name":"d"}
    ]
  },
  "username":"a",
  "uuid":[4,91,245,109,241,58,74,21,180,161,26,87,18,208,220,226]
  })";

constexpr auto mg_enterprise_no_license_json_str = R"({
    "databases":null,
    "fine_grained_access_handler":null,
    "password_hash":{"hash_algo":0,"password_hash":"$2a$12$pFMD3q0mfCg.lPD3ng0F5uzOCi5n4VZTDklBc2lQyXi19AaUwJXAa"},
    "permissions":{"denies":0,"grants":134217727},
    "user_imp":null,
    "username":"a",
    "uuid":[4,91,245,109,241,58,74,21,180,161,26,87,18,208,220,226]
    })";

constexpr auto community_json_str = R"({
      "password_hash":{"hash_algo":0,"password_hash":"$2a$12$pFMD3q0mfCg.lPD3ng0F5uzOCi5n4VZTDklBc2lQyXi19AaUwJXAa"},
      "permissions":{"denies":0,"grants":134217727},
      "username":"a",
      "uuid":[4,91,245,109,241,58,74,21,180,161,26,87,18,208,220,226]
      })";

constexpr auto community_saved_with_license_json_str = R"({
          "databases":{"allow_all":false,"default":"memgraph","denies":[],"grants":["memgraph"]},
          "fine_grained_access_handler":{
            "edge_type_permissions":{"global_permission":-1,"permissions":{}},
            "label_permissions":{"global_permission":-1,"permissions":{}}
          },
          "password_hash":{"hash_algo":0,"password_hash":"$2a$12$pFMD3q0mfCg.lPD3ng0F5uzOCi5n4VZTDklBc2lQyXi19AaUwJXAa"},
          "permissions":{"denies":0,"grants":134217727},
          "user_imp":null,
          "username":"a",
          "uuid":[4,91,245,109,241,58,74,21,180,161,26,87,18,208,220,226]
          })";

const memgraph::utils::UUID::arr_t hash({4, 91, 245, 109, 241, 58, 74, 21, 180, 161, 26, 87, 18, 208, 220, 226});

}  // namespace

template <typename UserOrRoleT>
class AuthModuleTest : public testing::Test {};

using UserOrRole = ::testing::Types<memgraph::auth::User, memgraph::auth::Role>;

TYPED_TEST_SUITE(AuthModuleTest, UserOrRole);

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(AuthModuleTest, Deserialization) {
  constexpr bool is_role = std::is_same_v<TypeParam, memgraph::auth::Role>;
  constexpr bool is_user = !is_role;

  auto full_json = nlohmann::json::parse(full_json_str);
  auto mg_enterprise_no_license_json = nlohmann::json::parse(mg_enterprise_no_license_json_str);
  auto community_json = nlohmann::json::parse(community_json_str);
  auto community_saved_with_license_json = nlohmann::json::parse(community_saved_with_license_json_str);

  if (is_role) {
    full_json = User2Role(full_json);
    mg_enterprise_no_license_json = User2Role(mg_enterprise_no_license_json);
    community_json = User2Role(community_json);
    community_saved_with_license_json = User2Role(community_saved_with_license_json);
  }

  // Verify JSON (with license)
  {
    // Startup test license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());

    auto auth_object = TypeParam::Deserialize(full_json);
    if constexpr (is_role) ASSERT_EQ(auth_object.rolename(), "a");
    if constexpr (is_user) {
      ASSERT_EQ(auth_object.username(), "a");
      ASSERT_EQ(memgraph::utils::UUID::arr_t(auth_object.uuid()), hash);
      ASSERT_TRUE(auth_object.CheckPasswordExplicit("password"));
    }
    ASSERT_EQ(auth_object.permissions().grants(), 134217727);
    ASSERT_EQ(auth_object.permissions().denies(), 0);
    const auto &etp = auth_object.fine_grained_access_handler().edge_type_permissions().GetGlobalPermission();
    ASSERT_TRUE(etp.has_value());
    ASSERT_EQ(etp.value(), 7);
    const auto etp_perm = std::unordered_map<std::string, uint64_t>{{"E", 1}};
    ASSERT_EQ(auth_object.fine_grained_access_handler().edge_type_permissions().GetPermissions(), etp_perm);
    const auto &lp = auth_object.fine_grained_access_handler().label_permissions().GetGlobalPermission();
    ASSERT_TRUE(lp.has_value());
    ASSERT_EQ(lp.value(), 7);
    const auto lp_perm = std::unordered_map<std::string, uint64_t>{{"A", 1}, {"B", 1}, {"C", 3}, {"D", 0}};
    ASSERT_EQ(auth_object.fine_grained_access_handler().label_permissions().GetPermissions(), lp_perm);
    ASSERT_EQ(auth_object.db_access().GetMain(), "db1");
    ASSERT_EQ(auth_object.db_access().GetAllowAll(), true);
    const auto grants = std::set<std::string, std::less<>>({"db1", "memgraph"});
    ASSERT_EQ(auth_object.db_access().GetGrants(), grants);
    const auto denies = std::set<std::string, std::less<>>({"db2", "db3"});
    ASSERT_EQ(auth_object.db_access().GetDenies(), denies);
    ASSERT_TRUE(auth_object.user_impersonation().has_value());
    ASSERT_TRUE(std::holds_alternative<std::set<memgraph::auth::UserImpersonation::UserId>>(
        auth_object.user_impersonation()->granted()));
    ASSERT_EQ(std::get<std::set<memgraph::auth::UserImpersonation::UserId>>(auth_object.user_impersonation()->granted())
                  .size(),
              2);
    ASSERT_EQ(auth_object.user_impersonation()->denied().size(), 1);

    auto auth_object_no_license = TypeParam::Deserialize(mg_enterprise_no_license_json);
    if constexpr (is_role) ASSERT_EQ(auth_object_no_license.rolename(), "a");
    if constexpr (is_user) {
      ASSERT_EQ(auth_object_no_license.username(), "a");
      ASSERT_EQ(memgraph::utils::UUID::arr_t(auth_object_no_license.uuid()), hash);
      ASSERT_TRUE(auth_object_no_license.CheckPasswordExplicit("password"));
    }
    ASSERT_EQ(auth_object_no_license.permissions().grants(), 134217727);
    ASSERT_EQ(auth_object_no_license.permissions().denies(), 0);
    ASSERT_FALSE(
        auth_object_no_license.fine_grained_access_handler().edge_type_permissions().GetGlobalPermission().has_value());
    ASSERT_FALSE(
        auth_object_no_license.fine_grained_access_handler().label_permissions().GetGlobalPermission().has_value());
    ASSERT_EQ(auth_object_no_license.db_access().GetMain(), "memgraph");
    ASSERT_EQ(auth_object_no_license.db_access().GetAllowAll(), false);
    ASSERT_TRUE(auth_object_no_license.db_access().GetGrants().size() == 1 &&
                *auth_object_no_license.db_access().GetGrants().begin() == "memgraph");
    ASSERT_TRUE(auth_object_no_license.db_access().GetDenies().empty());
    ASSERT_FALSE(auth_object_no_license.user_impersonation().has_value());

    auto auth_object_community = TypeParam::Deserialize(community_json);
    if constexpr (is_role) ASSERT_EQ(auth_object_community.rolename(), "a");
    if constexpr (is_user) {
      ASSERT_EQ(auth_object_community.username(), "a");
      ASSERT_EQ(memgraph::utils::UUID::arr_t(auth_object_community.uuid()), hash);
      ASSERT_TRUE(auth_object_community.CheckPasswordExplicit("password"));
    }
    ASSERT_EQ(auth_object_community.permissions().grants(), 134217727);
    ASSERT_EQ(auth_object_community.permissions().denies(), 0);
    ASSERT_FALSE(
        auth_object_community.fine_grained_access_handler().edge_type_permissions().GetGlobalPermission().has_value());
    ASSERT_FALSE(
        auth_object_community.fine_grained_access_handler().label_permissions().GetGlobalPermission().has_value());
    ASSERT_EQ(auth_object_community.db_access().GetMain(), "memgraph");
    ASSERT_EQ(auth_object_community.db_access().GetAllowAll(), false);
    ASSERT_TRUE(auth_object_community.db_access().GetGrants().size() == 1 &&
                *auth_object_community.db_access().GetGrants().begin() == "memgraph");
    ASSERT_TRUE(auth_object_community.db_access().GetDenies().empty());
    ASSERT_FALSE(auth_object_community.user_impersonation().has_value());

    auto auth_object_community_resaved = TypeParam::Deserialize(community_saved_with_license_json);
    if constexpr (is_role) ASSERT_EQ(auth_object_community_resaved.rolename(), "a");
    if constexpr (is_user) {
      ASSERT_EQ(auth_object_community_resaved.username(), "a");
      ASSERT_EQ(memgraph::utils::UUID::arr_t(auth_object_community_resaved.uuid()), hash);
      ASSERT_TRUE(auth_object_community_resaved.CheckPasswordExplicit("password"));
    }
    ASSERT_EQ(auth_object_community_resaved.permissions().grants(), 134217727);
    ASSERT_EQ(auth_object_community_resaved.permissions().denies(), 0);
    ASSERT_FALSE(auth_object_community_resaved.fine_grained_access_handler()
                     .edge_type_permissions()
                     .GetGlobalPermission()
                     .has_value());
    ASSERT_FALSE(auth_object_community_resaved.fine_grained_access_handler()
                     .label_permissions()
                     .GetGlobalPermission()
                     .has_value());
    ASSERT_EQ(auth_object_community_resaved.db_access().GetMain(), "memgraph");
    ASSERT_EQ(auth_object_community_resaved.db_access().GetAllowAll(), false);
    ASSERT_TRUE(auth_object_community_resaved.db_access().GetGrants().size() == 1 &&
                *auth_object_community_resaved.db_access().GetGrants().begin() == "memgraph");
    ASSERT_TRUE(auth_object_community_resaved.db_access().GetDenies().empty());
    ASSERT_FALSE(auth_object_community_resaved.user_impersonation().has_value());
  }

  // Verify JSON (no license)
  {
    // Revoke test license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());

    auto auth_object = TypeParam::Deserialize(full_json);
    if constexpr (is_role) ASSERT_EQ(auth_object.rolename(), "a");
    if constexpr (is_user) {
      ASSERT_EQ(auth_object.username(), "a");
      ASSERT_EQ(memgraph::utils::UUID::arr_t(auth_object.uuid()), hash);
      ASSERT_TRUE(auth_object.CheckPasswordExplicit("password"));
    }
    ASSERT_EQ(auth_object.permissions().grants(), 134217727);
    ASSERT_EQ(auth_object.permissions().denies(), 0);
    ASSERT_FALSE(auth_object.fine_grained_access_handler().edge_type_permissions().GetGlobalPermission().has_value());
    ASSERT_FALSE(auth_object.fine_grained_access_handler().label_permissions().GetGlobalPermission().has_value());
    ASSERT_EQ(auth_object.db_access().GetMain(), "memgraph");
    ASSERT_EQ(auth_object.db_access().GetAllowAll(), false);
    ASSERT_TRUE(auth_object.db_access().GetGrants().size() == 1 &&
                *auth_object.db_access().GetGrants().begin() == "memgraph");
    ASSERT_TRUE(auth_object.db_access().GetDenies().empty());
    ASSERT_FALSE(auth_object.user_impersonation().has_value());

    auto auth_object_no_license = TypeParam::Deserialize(mg_enterprise_no_license_json);
    if constexpr (is_role) ASSERT_EQ(auth_object_no_license.rolename(), "a");
    if constexpr (is_user) {
      ASSERT_EQ(auth_object_no_license.username(), "a");
      ASSERT_EQ(memgraph::utils::UUID::arr_t(auth_object_no_license.uuid()), hash);
      ASSERT_TRUE(auth_object_no_license.CheckPasswordExplicit("password"));
    }
    ASSERT_EQ(auth_object_no_license.permissions().grants(), 134217727);
    ASSERT_EQ(auth_object_no_license.permissions().denies(), 0);
    ASSERT_FALSE(
        auth_object_no_license.fine_grained_access_handler().edge_type_permissions().GetGlobalPermission().has_value());
    ASSERT_FALSE(
        auth_object_no_license.fine_grained_access_handler().label_permissions().GetGlobalPermission().has_value());
    ASSERT_EQ(auth_object_no_license.db_access().GetMain(), "memgraph");
    ASSERT_EQ(auth_object_no_license.db_access().GetAllowAll(), false);
    ASSERT_TRUE(auth_object_no_license.db_access().GetGrants().size() == 1 &&
                *auth_object_no_license.db_access().GetGrants().begin() == "memgraph");
    ASSERT_TRUE(auth_object_no_license.db_access().GetDenies().empty());
    ASSERT_FALSE(auth_object_no_license.user_impersonation().has_value());

    auto auth_object_community = TypeParam::Deserialize(community_json);
    if constexpr (is_role) ASSERT_EQ(auth_object_community.rolename(), "a");
    if constexpr (is_user) {
      ASSERT_EQ(auth_object_community.username(), "a");
      ASSERT_EQ(memgraph::utils::UUID::arr_t(auth_object_community.uuid()), hash);
      ASSERT_TRUE(auth_object_community.CheckPasswordExplicit("password"));
    }
    ASSERT_EQ(auth_object_community.permissions().grants(), 134217727);
    ASSERT_EQ(auth_object_community.permissions().denies(), 0);
    ASSERT_FALSE(
        auth_object_community.fine_grained_access_handler().edge_type_permissions().GetGlobalPermission().has_value());
    ASSERT_FALSE(
        auth_object_community.fine_grained_access_handler().label_permissions().GetGlobalPermission().has_value());
    ASSERT_EQ(auth_object_community.db_access().GetMain(), "memgraph");
    ASSERT_EQ(auth_object_community.db_access().GetAllowAll(), false);
    ASSERT_TRUE(auth_object_community.db_access().GetGrants().size() == 1 &&
                *auth_object_community.db_access().GetGrants().begin() == "memgraph");
    ASSERT_TRUE(auth_object_community.db_access().GetDenies().empty());
    ASSERT_FALSE(auth_object_community.user_impersonation().has_value());

    auto auth_object_community_resaved = TypeParam::Deserialize(community_saved_with_license_json);
    if constexpr (is_role) ASSERT_EQ(auth_object_community_resaved.rolename(), "a");
    if constexpr (is_user) {
      ASSERT_EQ(auth_object_community_resaved.username(), "a");
      ASSERT_EQ(memgraph::utils::UUID::arr_t(auth_object_community_resaved.uuid()), hash);
      ASSERT_TRUE(auth_object_community_resaved.CheckPasswordExplicit("password"));
    }
    ASSERT_EQ(auth_object_community_resaved.permissions().grants(), 134217727);
    ASSERT_EQ(auth_object_community_resaved.permissions().denies(), 0);
    ASSERT_FALSE(auth_object_community_resaved.fine_grained_access_handler()
                     .edge_type_permissions()
                     .GetGlobalPermission()
                     .has_value());
    ASSERT_FALSE(auth_object_community_resaved.fine_grained_access_handler()
                     .label_permissions()
                     .GetGlobalPermission()
                     .has_value());
    ASSERT_EQ(auth_object_community_resaved.db_access().GetMain(), "memgraph");
    ASSERT_EQ(auth_object_community_resaved.db_access().GetAllowAll(), false);
    ASSERT_TRUE(auth_object_community_resaved.db_access().GetGrants().size() == 1 &&
                *auth_object_community_resaved.db_access().GetGrants().begin() == "memgraph");
    ASSERT_TRUE(auth_object_community_resaved.db_access().GetDenies().empty());
    ASSERT_FALSE(auth_object_community_resaved.user_impersonation().has_value());
  }

  // Empty JSON (throw)
  {
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(TypeParam::Deserialize(nlohmann::json{}), memgraph::auth::AuthException);
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(TypeParam::Deserialize(nlohmann::json{}), memgraph::auth::AuthException);
  }

  // Missing username (throw)
  if constexpr (is_user) {
    auto json = full_json;
    json.erase(kUsername);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(memgraph::auth::User::Deserialize(json), memgraph::auth::AuthException);
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(memgraph::auth::User::Deserialize(json), memgraph::auth::AuthException);
  } else {
    auto json = full_json;
    json.erase(kRoleName);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(memgraph::auth::Role::Deserialize(json), memgraph::auth::AuthException);
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(memgraph::auth::Role::Deserialize(json), memgraph::auth::AuthException);
  }

  // Missing permissions (throw)
  {
    auto json = full_json;
    json.erase(kPermissions);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(TypeParam::Deserialize(json), memgraph::auth::AuthException);
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(TypeParam::Deserialize(json), memgraph::auth::AuthException);
  }

  // Missing password hash (throw)
  if constexpr (is_user) {
    auto json = full_json;
    json.erase(kPasswordHash);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(memgraph::auth::User::Deserialize(json), memgraph::auth::AuthException);
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(memgraph::auth::User::Deserialize(json), memgraph::auth::AuthException);
  }

  // Null password
  if constexpr (is_user) {
    // Startup test license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    {
      auto json = full_json;
      json[kPasswordHash] = {};
      auto user = memgraph::auth::User::Deserialize(json);
      ASSERT_TRUE(user.CheckPasswordExplicit(""));
    }
    {
      auto json = mg_enterprise_no_license_json;
      json[kPasswordHash] = {};
      auto user = memgraph::auth::User::Deserialize(json);
      ASSERT_TRUE(user.CheckPasswordExplicit(""));
    }
    {
      auto json = community_json;
      json[kPasswordHash] = {};
      auto user = memgraph::auth::User::Deserialize(json);
      ASSERT_TRUE(user.CheckPasswordExplicit(""));
    }
    // Reset test license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    {
      auto json = full_json;
      json[kPasswordHash] = {};
      auto user = memgraph::auth::User::Deserialize(json);
      ASSERT_TRUE(user.CheckPasswordExplicit(""));
    }
    {
      auto json = mg_enterprise_no_license_json;
      json[kPasswordHash] = {};
      auto user = memgraph::auth::User::Deserialize(json);
      ASSERT_TRUE(user.CheckPasswordExplicit(""));
    }
    {
      auto json = community_json;
      json[kPasswordHash] = {};
      auto user = memgraph::auth::User::Deserialize(json);
      ASSERT_TRUE(user.CheckPasswordExplicit(""));
    }
  }

  // Missing hash algo (throw)
  if constexpr (is_user) {
    auto json = full_json;
    json[kPasswordHash].erase(kHashAlgo);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(memgraph::auth::User::Deserialize(json), memgraph::auth::AuthException);
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(memgraph::auth::User::Deserialize(json), memgraph::auth::AuthException);
  }

  // Missing hash value (throw)
  if constexpr (is_user) {
    auto json = full_json;
    json[kPasswordHash].erase(kPasswordHash);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(memgraph::auth::User::Deserialize(json), memgraph::auth::AuthException);
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(memgraph::auth::User::Deserialize(json), memgraph::auth::AuthException);
  }

  // Missing UUID ( default to random UUID )
  if constexpr (is_user) {
    auto json = full_json;
    json.erase(kUUID);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_NO_THROW(memgraph::auth::User::Deserialize(json));
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_NO_THROW(memgraph::auth::User::Deserialize(json));
  }

  // Missing fine grained access handler ( default to no access )
  {
    auto json = full_json;
    json.erase(kFineGrainedAccessHandler);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    const auto auth_object_license = TypeParam::Deserialize(json);
    ASSERT_FALSE(
        auth_object_license.fine_grained_access_handler().edge_type_permissions().GetGlobalPermission().has_value());
    ASSERT_FALSE(
        auth_object_license.fine_grained_access_handler().label_permissions().GetGlobalPermission().has_value());
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    const auto auth_object_no_license = TypeParam::Deserialize(json);
    ASSERT_FALSE(
        auth_object_no_license.fine_grained_access_handler().edge_type_permissions().GetGlobalPermission().has_value());
    ASSERT_FALSE(
        auth_object_no_license.fine_grained_access_handler().label_permissions().GetGlobalPermission().has_value());
  }

  // Missing edge permissions fine grained access handler (throw)
  {
    auto json = full_json;
    json[kFineGrainedAccessHandler].erase(kEdgeTypePermissions);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(TypeParam::Deserialize(json), memgraph::auth::AuthException);
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_NO_THROW(TypeParam::Deserialize(json));
  }
  // TODO This should throw, the key exists, but the value is broken
  // Missing edge global permissions fine grained access handler ( default to no access )
  {
    auto json = full_json;
    json[kFineGrainedAccessHandler][kEdgeTypePermissions].erase(kGlobalPermission);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    const auto auth_object_license = TypeParam::Deserialize(json);
    ASSERT_FALSE(
        auth_object_license.fine_grained_access_handler().edge_type_permissions().GetGlobalPermission().has_value());
    ASSERT_FALSE(auth_object_license.fine_grained_access_handler().edge_type_permissions().GetPermissions().empty());
    ASSERT_TRUE(
        auth_object_license.fine_grained_access_handler().label_permissions().GetGlobalPermission().has_value());
    ASSERT_FALSE(auth_object_license.fine_grained_access_handler().label_permissions().GetPermissions().empty());
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    const auto auth_object_no_license = TypeParam::Deserialize(json);
    ASSERT_FALSE(
        auth_object_no_license.fine_grained_access_handler().edge_type_permissions().GetGlobalPermission().has_value());
    ASSERT_TRUE(auth_object_no_license.fine_grained_access_handler().edge_type_permissions().GetPermissions().empty());
    ASSERT_FALSE(
        auth_object_no_license.fine_grained_access_handler().label_permissions().GetGlobalPermission().has_value());
    ASSERT_TRUE(auth_object_no_license.fine_grained_access_handler().label_permissions().GetPermissions().empty());
  }

  // TODO This should throw, the key exists, but the value is broken
  // Missing edge local permissions fine grained access handler ( default to no access )
  {
    auto json = full_json;
    json[kFineGrainedAccessHandler][kEdgeTypePermissions].erase(kPermissions);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    const auto auth_object_license = TypeParam::Deserialize(json);
    ASSERT_TRUE(
        auth_object_license.fine_grained_access_handler().edge_type_permissions().GetGlobalPermission().has_value());
    ASSERT_TRUE(auth_object_license.fine_grained_access_handler().edge_type_permissions().GetPermissions().empty());
    ASSERT_TRUE(
        auth_object_license.fine_grained_access_handler().label_permissions().GetGlobalPermission().has_value());
    ASSERT_FALSE(auth_object_license.fine_grained_access_handler().label_permissions().GetPermissions().empty());
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    const auto auth_object_no_license = TypeParam::Deserialize(json);
    ASSERT_FALSE(
        auth_object_no_license.fine_grained_access_handler().edge_type_permissions().GetGlobalPermission().has_value());
    ASSERT_TRUE(auth_object_no_license.fine_grained_access_handler().edge_type_permissions().GetPermissions().empty());
    ASSERT_FALSE(
        auth_object_no_license.fine_grained_access_handler().label_permissions().GetGlobalPermission().has_value());
    ASSERT_TRUE(auth_object_no_license.fine_grained_access_handler().label_permissions().GetPermissions().empty());
  }

  // Missing label permissions fine grained access handler (throw)
  {
    auto json = full_json;
    json[kFineGrainedAccessHandler].erase(kLabelPermissions);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(TypeParam::Deserialize(json), memgraph::auth::AuthException);
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_NO_THROW(TypeParam::Deserialize(json));
  }

  // TODO This should throw, the key exists, but the value is broken
  // Missing label global permissions fine grained access handler ( default to no access )
  {
    auto json = full_json;
    json[kFineGrainedAccessHandler][kLabelPermissions].erase(kGlobalPermission);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    const auto auth_object_license = TypeParam::Deserialize(json);
    ASSERT_TRUE(
        auth_object_license.fine_grained_access_handler().edge_type_permissions().GetGlobalPermission().has_value());
    ASSERT_FALSE(auth_object_license.fine_grained_access_handler().edge_type_permissions().GetPermissions().empty());
    ASSERT_FALSE(
        auth_object_license.fine_grained_access_handler().label_permissions().GetGlobalPermission().has_value());
    ASSERT_FALSE(auth_object_license.fine_grained_access_handler().label_permissions().GetPermissions().empty());
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    const auto auth_object_no_license = TypeParam::Deserialize(json);
    ASSERT_FALSE(
        auth_object_no_license.fine_grained_access_handler().edge_type_permissions().GetGlobalPermission().has_value());
    ASSERT_TRUE(auth_object_no_license.fine_grained_access_handler().edge_type_permissions().GetPermissions().empty());
    ASSERT_FALSE(
        auth_object_no_license.fine_grained_access_handler().label_permissions().GetGlobalPermission().has_value());
    ASSERT_TRUE(auth_object_no_license.fine_grained_access_handler().label_permissions().GetPermissions().empty());
  }

  // TODO This should throw, the key exists, but the value is broken
  // Missing label local permissions fine grained access handler ( default to no access )
  {
    auto json = full_json;
    json[kFineGrainedAccessHandler][kLabelPermissions].erase(kPermissions);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    const auto auth_object_license = TypeParam::Deserialize(json);
    ASSERT_TRUE(
        auth_object_license.fine_grained_access_handler().edge_type_permissions().GetGlobalPermission().has_value());
    ASSERT_FALSE(auth_object_license.fine_grained_access_handler().edge_type_permissions().GetPermissions().empty());
    ASSERT_TRUE(
        auth_object_license.fine_grained_access_handler().label_permissions().GetGlobalPermission().has_value());
    ASSERT_TRUE(auth_object_license.fine_grained_access_handler().label_permissions().GetPermissions().empty());
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    const auto auth_object_no_license = TypeParam::Deserialize(json);
    ASSERT_FALSE(
        auth_object_no_license.fine_grained_access_handler().edge_type_permissions().GetGlobalPermission().has_value());
    ASSERT_TRUE(auth_object_no_license.fine_grained_access_handler().edge_type_permissions().GetPermissions().empty());
    ASSERT_FALSE(
        auth_object_no_license.fine_grained_access_handler().label_permissions().GetGlobalPermission().has_value());
    ASSERT_TRUE(auth_object_no_license.fine_grained_access_handler().label_permissions().GetPermissions().empty());
  }

  // Missing database access ( default to memgraph )
  {
    auto json = full_json;
    json.erase(kDatabases);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    const auto auth_object_license = TypeParam::Deserialize(json);
    ASSERT_EQ(auth_object_license.db_access().GetMain(), "memgraph");
    ASSERT_EQ(auth_object_license.db_access().GetAllowAll(), false);
    ASSERT_TRUE(auth_object_license.db_access().GetGrants().size() == 1 &&
                *auth_object_license.db_access().GetGrants().begin() == "memgraph");
    ASSERT_TRUE(auth_object_license.db_access().GetDenies().empty());
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    const auto auth_object_no_license = TypeParam::Deserialize(json);
    ASSERT_EQ(auth_object_no_license.db_access().GetMain(), "memgraph");
    ASSERT_EQ(auth_object_no_license.db_access().GetAllowAll(), false);
    ASSERT_TRUE(auth_object_no_license.db_access().GetGrants().size() == 1 &&
                *auth_object_no_license.db_access().GetGrants().begin() == "memgraph");
    ASSERT_TRUE(auth_object_no_license.db_access().GetDenies().empty());
  }

  // Missing database access allow all (throw)
  {
    auto json = full_json;
    json[kDatabases].erase(kAllowAll);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(TypeParam::Deserialize(json), memgraph::auth::AuthException);
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_NO_THROW(TypeParam::Deserialize(json));
  }

  // Missing database access grants (throw)
  {
    auto json = full_json;
    json[kDatabases].erase(kGrants);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(TypeParam::Deserialize(json), memgraph::auth::AuthException);
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_NO_THROW(TypeParam::Deserialize(json));
  }

  // Missing database access denies (throw)
  {
    auto json = full_json;
    json[kDatabases].erase(kDenies);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(TypeParam::Deserialize(json), memgraph::auth::AuthException);
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_NO_THROW(TypeParam::Deserialize(json));
  }

  // Missing database access main (throw)
  {
    auto json = full_json;
    json[kDatabases].erase(kDefault);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(TypeParam::Deserialize(json), memgraph::auth::AuthException);
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_NO_THROW(TypeParam::Deserialize(json));
  }

  // Missing user impersonation ( default to no user impersonation )
  {
    auto json = full_json;
    json.erase(kUserImp);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    const auto auth_object_license = TypeParam::Deserialize(json);
    ASSERT_FALSE(auth_object_license.user_impersonation().has_value());
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    const auto auth_object_no_license = TypeParam::Deserialize(json);
    ASSERT_FALSE(auth_object_no_license.user_impersonation().has_value());
  }

  // Grant all user impersonation
  {
    auto json = full_json;
    json[kUserImp][kUserImpGranted] = nlohmann::json::object();
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    const auto auth_object_license = TypeParam::Deserialize(json);
    ASSERT_TRUE(auth_object_license.user_impersonation().has_value());
    ASSERT_TRUE(std::holds_alternative<memgraph::auth::UserImpersonation::GrantAllUsers>(
        auth_object_license.user_impersonation()->granted()));
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    const auto auth_object_no_license = TypeParam::Deserialize(json);
    ASSERT_FALSE(auth_object_no_license.user_impersonation().has_value());
  }

  // Grant no user impersonation
  {
    auto json = full_json;
    json[kUserImp][kUserImpGranted] = nlohmann::json::array();
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    const auto auth_object_license = TypeParam::Deserialize(json);
    ASSERT_TRUE(auth_object_license.user_impersonation().has_value());
    ASSERT_TRUE(std::holds_alternative<std::set<memgraph::auth::UserImpersonation::UserId>>(
        auth_object_license.user_impersonation()->granted()));
    ASSERT_TRUE(std::get<std::set<memgraph::auth::UserImpersonation::UserId>>(
                    auth_object_license.user_impersonation()->granted())
                    .empty());
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    const auto auth_object_no_license = TypeParam::Deserialize(json);
    ASSERT_FALSE(auth_object_no_license.user_impersonation().has_value());
  }

  // Missing user impersonation granted (throw)
  {
    auto json = full_json;
    json[kUserImp].erase(kUserImpGranted);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(TypeParam::Deserialize(json), memgraph::auth::AuthException);
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_NO_THROW(TypeParam::Deserialize(json));
  }

  // Missing user impersonation granted uuid (throw)
  {
    auto json = full_json;
    json[kUserImp][kUserImpGranted][0].erase(kUserImpId);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(TypeParam::Deserialize(json), memgraph::auth::AuthException);
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_NO_THROW(TypeParam::Deserialize(json));
  }

  // Missing user impersonation granted name (throw)
  {
    auto json = full_json;
    json[kUserImp][kUserImpGranted][0].erase(kUserImpName);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(TypeParam::Deserialize(json), memgraph::auth::AuthException);
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_NO_THROW(TypeParam::Deserialize(json));
  }

  // Missing user impersonation denied (throw)
  {
    auto json = full_json;
    json[kUserImp].erase(kUserImpDenied);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(TypeParam::Deserialize(json), memgraph::auth::AuthException);
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_NO_THROW(TypeParam::Deserialize(json));
  }

  // Missing user impersonation denied uuid (throw)
  {
    auto json = full_json;
    json[kUserImp][kUserImpDenied][0].erase(kUserImpId);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(TypeParam::Deserialize(json), memgraph::auth::AuthException);
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_NO_THROW(TypeParam::Deserialize(json));
  }

  // Missing user impersonation denied name (throw)
  {
    auto json = full_json;
    json[kUserImp][kUserImpDenied][0].erase(kUserImpName);
    // With license
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(TypeParam::Deserialize(json), memgraph::auth::AuthException);
    // Without license
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_NO_THROW(TypeParam::Deserialize(json));
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TYPED_TEST(AuthModuleTest, Reserialization) {
  constexpr bool is_role = std::is_same_v<TypeParam, memgraph::auth::Role>;

  auto full_json = nlohmann::json::parse(full_json_str);
  auto mg_enterprise_no_license_json = nlohmann::json::parse(mg_enterprise_no_license_json_str);
  auto community_json = nlohmann::json::parse(community_json_str);
  auto community_saved_with_license_json = nlohmann::json::parse(community_saved_with_license_json_str);

  if (is_role) {
    full_json = User2Role(full_json);
    mg_enterprise_no_license_json = User2Role(mg_enterprise_no_license_json);
    community_json = User2Role(community_json);
    community_saved_with_license_json = User2Role(community_saved_with_license_json);
  }

  // Re-serialize user (with license)
  {
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());

    // Full object should be same as original
    auto full_object = TypeParam::Deserialize(full_json);
    EXPECT_EQ(full_json, full_object.Serialize());

    // Created without license, missing fields should default to the same null values
    auto no_license_object = TypeParam::Deserialize(mg_enterprise_no_license_json);
    EXPECT_EQ(community_saved_with_license_json, no_license_object.Serialize());

    // Created in community version; missing fields should be inserted back (as in no_license example)
    auto community_object = TypeParam::Deserialize(community_json);
    EXPECT_EQ(community_saved_with_license_json, community_object.Serialize());
  }

  // Re-serialize user (without license)
  {
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());

    // Created with license; enterprise fields should be null
    auto full_object = TypeParam::Deserialize(full_json);
    EXPECT_EQ(mg_enterprise_no_license_json, full_object.Serialize());

    // Created without license, missing fields should default to the same null values
    auto no_license_object = TypeParam::Deserialize(mg_enterprise_no_license_json);
    EXPECT_EQ(mg_enterprise_no_license_json, no_license_object.Serialize());

    // Created in community version; missing fields should be inserted back (as in no_license example)
    auto community_object = TypeParam::Deserialize(community_json);
    EXPECT_EQ(mg_enterprise_no_license_json, community_object.Serialize());
  }
}

TEST(AuthModule, UserSerialization) {
  memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
  ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());

  auto json = nlohmann::json::parse(R"({
          "databases":{"allow_all":false,"default":"memgraph","denies":[],"grants":["memgraph"]},
          "fine_grained_access_handler":{
            "edge_type_permissions":{"global_permission":-1,"permissions":{}},
            "label_permissions":{"global_permission":-1,"permissions":{}}
          },
          "password_hash":null,
          "permissions":{"denies":0,"grants":0},
          "user_imp":null,
          "username":"",
          "uuid":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]
          })");

  // Empty user
  {
    memgraph::auth::User user{};
    // Need to update UUID (no way to inject)
    json[kUUID] = user.uuid();
    ASSERT_EQ(json, user.Serialize());
  }

  // User with username, no password
  {
    memgraph::auth::User user{"test_user"};
    // Need to update UUID (no way to inject)
    json[kUUID] = user.uuid();
    json[kUsername] = "test_user";
    ASSERT_EQ(json, user.Serialize());
  }

  // User with password
  memgraph::auth::User user{"test_user",
                            memgraph::auth::HashedPassword{memgraph::auth::PasswordHashAlgorithm::SHA256, "anything"},
                            memgraph::auth::Permissions{/* no permissions */}};
  // Need to update UUID (no way to inject)
  json[kUUID] = user.uuid();
  json[kUsername] = "test_user";
  json[kPasswordHash] = nlohmann::json::object({{"hash_algo", 1}, {"password_hash", "anything"}});
  ASSERT_EQ(json, user.Serialize());

  // User with permissions
  user.GetPermissions().Deny(memgraph::auth::Permission::CONSTRAINT);
  json[kPermissions][kDenies] = user.GetPermissions().denies();
  ASSERT_EQ(json, user.Serialize());
  user.GetPermissions().Grant(memgraph::auth::Permission::DELETE);
  user.GetPermissions().Grant(memgraph::auth::Permission::AUTH);
  json[kPermissions][kGrants] = user.GetPermissions().grants();
  ASSERT_EQ(json, user.Serialize());

  // User with fine grained permissions
  user.fine_grained_access_handler().edge_type_permissions().Grant("ABC", memgraph::auth::FineGrainedPermission::READ);
  json[kFineGrainedAccessHandler][kEdgeTypePermissions][kPermissions] = nlohmann::json::object({{"ABC", 1}});
  ASSERT_EQ(json, user.Serialize());
  user.fine_grained_access_handler().label_permissions().Grant("CBA", memgraph::auth::FineGrainedPermission::NOTHING);
  json[kFineGrainedAccessHandler][kLabelPermissions][kPermissions] = nlohmann::json::object({{"CBA", 0}});
  ASSERT_EQ(json, user.Serialize());

  // User with MT access
  user.db_access().Grant("db1");
  json[kDatabases][kGrants] = nlohmann::json::array({"db1", "memgraph"});
  ASSERT_EQ(json, user.Serialize());
  user.db_access().SetMain("db1");
  json[kDatabases][kDefault] = "db1";
  ASSERT_EQ(json, user.Serialize());
  user.db_access().Deny("db2");
  user.db_access().Deny("db3");
  json[kDatabases][kDenies] = nlohmann::json::array({"db2", "db3"});
  ASSERT_EQ(json, user.Serialize());
  user.db_access().DenyAll();
  json[kDatabases][kGrants] = nlohmann::json::array();
  json[kDatabases][kDenies] = nlohmann::json::array();
  ASSERT_EQ(json, user.Serialize());
  user.db_access().GrantAll();
  json[kDatabases][kAllowAll] = true;
  ASSERT_EQ(json, user.Serialize());
  user.db_access().Deny("db3");
  json[kDatabases][kDenies] = nlohmann::json::array({"db3"});
  ASSERT_EQ(json, user.Serialize());

  // User with user impersonation
  memgraph::auth::User user1{"user1"};
  memgraph::auth::User user2{"user2"};
  memgraph::auth::User user3{"user3"};
  user.DenyUserImp({user2, user1});
  json[kUserImp][kUserImpGranted] = nlohmann::json::array();
  json[kUserImp][kUserImpDenied] = nlohmann::json::array(
      {{{kUserImpName, "user1"}, {kUserImpId, user1.uuid()}}, {{kUserImpName, "user2"}, {kUserImpId, user2.uuid()}}});
  ASSERT_EQ(json, user.Serialize());
  user.GrantUserImp({user3, user1});
  json[kUserImp][kUserImpGranted] = nlohmann::json::array(
      {{{kUserImpName, "user1"}, {kUserImpId, user1.uuid()}}, {{kUserImpName, "user3"}, {kUserImpId, user3.uuid()}}});
  json[kUserImp][kUserImpDenied] = nlohmann::json::array({{{kUserImpName, "user2"}, {kUserImpId, user2.uuid()}}});
  ASSERT_EQ(json, user.Serialize());
}

TEST(AuthModule, RoleSerialization) {
  memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
  ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());

  auto json = nlohmann::json::parse(R"({
          "databases":{"allow_all":false,"default":"memgraph","denies":[],"grants":["memgraph"]},
          "fine_grained_access_handler":{
            "edge_type_permissions":{"global_permission":-1,"permissions":{}},
            "label_permissions":{"global_permission":-1,"permissions":{}}
          },
          "permissions":{"denies":0,"grants":0},
          "rolename":"",
          "user_imp":null
          })");

  // Empty role
  {
    memgraph::auth::Role role{};
    ASSERT_EQ(json, role.Serialize());
  }

  // Role with rolename, no password
  {
    memgraph::auth::Role role{"test_role"};
    json[kRoleName] = "test_role";
    ASSERT_EQ(json, role.Serialize());
  }

  // Role with password
  memgraph::auth::Role role{"test_role", memgraph::auth::Permissions{/* no permissions */}};
  json[kRoleName] = "test_role";
  ASSERT_EQ(json, role.Serialize());

  // Role with permissions
  role.GetPermissions().Deny(memgraph::auth::Permission::CONSTRAINT);
  json[kPermissions][kDenies] = role.GetPermissions().denies();
  ASSERT_EQ(json, role.Serialize());
  role.GetPermissions().Grant(memgraph::auth::Permission::DELETE);
  role.GetPermissions().Grant(memgraph::auth::Permission::AUTH);
  json[kPermissions][kGrants] = role.GetPermissions().grants();
  ASSERT_EQ(json, role.Serialize());

  // Role with fine grained permissions
  role.fine_grained_access_handler().edge_type_permissions().Grant("ABC", memgraph::auth::FineGrainedPermission::READ);
  json[kFineGrainedAccessHandler][kEdgeTypePermissions][kPermissions] = nlohmann::json::object({{"ABC", 1}});
  ASSERT_EQ(json, role.Serialize());
  role.fine_grained_access_handler().label_permissions().Grant("CBA", memgraph::auth::FineGrainedPermission::NOTHING);
  json[kFineGrainedAccessHandler][kLabelPermissions][kPermissions] = nlohmann::json::object({{"CBA", 0}});
  ASSERT_EQ(json, role.Serialize());

  // Role with MT access
  role.db_access().Grant("db1");
  json[kDatabases][kGrants] = nlohmann::json::array({"db1", "memgraph"});
  ASSERT_EQ(json, role.Serialize());
  role.db_access().SetMain("db1");
  json[kDatabases][kDefault] = "db1";
  ASSERT_EQ(json, role.Serialize());
  role.db_access().Deny("db2");
  role.db_access().Deny("db3");
  json[kDatabases][kDenies] = nlohmann::json::array({"db2", "db3"});
  ASSERT_EQ(json, role.Serialize());
  role.db_access().DenyAll();
  json[kDatabases][kGrants] = nlohmann::json::array();
  json[kDatabases][kDenies] = nlohmann::json::array();
  ASSERT_EQ(json, role.Serialize());
  role.db_access().GrantAll();
  json[kDatabases][kAllowAll] = true;
  ASSERT_EQ(json, role.Serialize());
  role.db_access().Deny("db3");
  json[kDatabases][kDenies] = nlohmann::json::array({"db3"});
  ASSERT_EQ(json, role.Serialize());

  // Role with user impersonation
  memgraph::auth::User user1{"user1"};
  memgraph::auth::User user2{"user2"};
  memgraph::auth::User user3{"user3"};
  role.DenyUserImp({user2, user1});
  json[kUserImp][kUserImpGranted] = nlohmann::json::array();
  json[kUserImp][kUserImpDenied] = nlohmann::json::array(
      {{{kUserImpName, "user1"}, {kUserImpId, user1.uuid()}}, {{kUserImpName, "user2"}, {kUserImpId, user2.uuid()}}});
  ASSERT_EQ(json, role.Serialize());
  role.GrantUserImp({user3, user1});
  json[kUserImp][kUserImpGranted] = nlohmann::json::array(
      {{{kUserImpName, "user1"}, {kUserImpId, user1.uuid()}}, {{kUserImpName, "user3"}, {kUserImpId, user3.uuid()}}});
  json[kUserImp][kUserImpDenied] = nlohmann::json::array({{{kUserImpName, "user2"}, {kUserImpId, user2.uuid()}}});
  ASSERT_EQ(json, role.Serialize());
}

#ifdef MG_ENTERPRISE
TEST(AuthModule, UserProfiles) {
  memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
  ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());

  // Test UserProfiles class directly since profile management is now centralized
  auto temp_dir = std::filesystem::temp_directory_path() / "MG_test_user_profiles";

  // Clean up any existing directory
  if (std::filesystem::exists(temp_dir)) {
    std::filesystem::remove_all(temp_dir);
  }

  memgraph::kvstore::KVStore kvstore{temp_dir};
  memgraph::auth::UserProfiles user_profiles{kvstore};

  // Test profile creation
  ASSERT_TRUE(user_profiles.Create("profile", {}));
  ASSERT_TRUE(user_profiles.Create("other_profile", {{memgraph::auth::UserProfiles::Limits::kSessions,
                                                      memgraph::auth::UserProfiles::limit_t{1UL}}}));

  // Test profile creation with usernames
  ASSERT_TRUE(user_profiles.Create("profile_with_users", {}, {"user1", "user2", "user3"}));
  auto profile_with_users = user_profiles.Get("profile_with_users");
  ASSERT_TRUE(profile_with_users.has_value());
  ASSERT_EQ(profile_with_users->usernames.size(), 3);
  ASSERT_TRUE(profile_with_users->usernames.find("user1") != profile_with_users->usernames.end());
  ASSERT_TRUE(profile_with_users->usernames.find("user2") != profile_with_users->usernames.end());
  ASSERT_TRUE(profile_with_users->usernames.find("user3") != profile_with_users->usernames.end());

  // Test that usernames are moved from other profiles when creating a new profile
  ASSERT_TRUE(user_profiles.Create("profile1", {}, {"user1", "user4"}));
  ASSERT_TRUE(user_profiles.Create("profile2", {}, {"user1", "user5"}));  // user1 should be moved from profile1

  auto profile1 = user_profiles.Get("profile1");
  auto profile2 = user_profiles.Get("profile2");
  ASSERT_TRUE(profile1.has_value());
  ASSERT_TRUE(profile2.has_value());

  // user1 should be moved from profile1 to profile2
  ASSERT_TRUE(profile1->usernames.find("user1") == profile1->usernames.end());
  ASSERT_TRUE(profile1->usernames.find("user4") != profile1->usernames.end());
  ASSERT_TRUE(profile2->usernames.find("user1") != profile2->usernames.end());
  ASSERT_TRUE(profile2->usernames.find("user5") != profile2->usernames.end());

  // Test profile retrieval
  auto profile = user_profiles.Get("profile");
  ASSERT_TRUE(profile.has_value());
  ASSERT_EQ(profile->name, "profile");
  ASSERT_EQ(profile->limits.size(), 0);

  auto other_profile = user_profiles.Get("other_profile");
  ASSERT_TRUE(other_profile.has_value());
  ASSERT_EQ(other_profile->name, "other_profile");
  ASSERT_EQ(other_profile->limits.size(), 1);

  // Test username management
  ASSERT_TRUE(user_profiles.AddUsername("profile", "user1"));
  ASSERT_TRUE(user_profiles.AddUsername("profile", "user2"));
  ASSERT_TRUE(user_profiles.AddUsername("other_profile", "user3"));

  // Test getting usernames for profile
  auto usernames = user_profiles.GetUsernames("profile");
  ASSERT_EQ(usernames.size(), 2);
  ASSERT_TRUE(std::find(usernames.begin(), usernames.end(), "user1") != usernames.end());
  ASSERT_TRUE(std::find(usernames.begin(), usernames.end(), "user2") != usernames.end());

  // Test getting profile for username
  auto profile_for_user = user_profiles.GetProfileForUsername("user1");
  ASSERT_TRUE(profile_for_user.has_value());
  ASSERT_EQ(*profile_for_user, "profile");

  // Test removing username
  ASSERT_TRUE(user_profiles.RemoveUsername("profile", "user1"));
  usernames = user_profiles.GetUsernames("profile");
  ASSERT_EQ(usernames.size(), 1);
  ASSERT_TRUE(usernames.find("user2") != usernames.end());

  // Test profile update
  ASSERT_TRUE(user_profiles.Update(
      "profile", {{memgraph::auth::UserProfiles::Limits::kSessions, memgraph::auth::UserProfiles::limit_t{5UL}}}));
  profile = user_profiles.Get("profile");
  ASSERT_TRUE(profile.has_value());
  ASSERT_EQ(profile->limits.size(), 1);

  // Test profile deletion
  ASSERT_TRUE(user_profiles.Drop("profile"));
  profile = user_profiles.Get("profile");
  ASSERT_FALSE(profile.has_value());

  // Clean up test directory
  if (std::filesystem::exists(temp_dir)) {
    std::filesystem::remove_all(temp_dir);
  }
}
#endif
