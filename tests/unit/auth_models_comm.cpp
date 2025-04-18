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

#include "auth/crypto.hpp"
#include "auth/exceptions.hpp"
#include "auth/models.hpp"
#include "license/license.hpp"
#include "nlohmann/json_fwd.hpp"

namespace {
constexpr auto kPermissions = "permissions";
constexpr auto kGrants = "grants";
constexpr auto kDenies = "denies";
constexpr auto kUsername = "username";
constexpr auto kUUID = "uuid";
constexpr auto kPasswordHash = "password_hash";
constexpr auto kHashAlgo = "hash_algo";

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

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(AuthModule, Deserialization) {
  auto full_json = nlohmann::json::parse(full_json_str);
  auto mg_enterprise_no_license_json = nlohmann::json::parse(mg_enterprise_no_license_json_str);
  auto community_json = nlohmann::json::parse(community_json_str);
  auto community_saved_with_license_json = nlohmann::json::parse(community_saved_with_license_json_str);

  // Verify JSON
  {
    auto auth_object = memgraph::auth::User::Deserialize(full_json);
    ASSERT_EQ(auth_object.username(), "a");
    ASSERT_EQ(memgraph::utils::UUID::arr_t(auth_object.uuid()), hash);
    ASSERT_TRUE(auth_object.CheckPasswordExplicit("password"));
    ASSERT_EQ(auth_object.permissions().grants(), 134217727);
    ASSERT_EQ(auth_object.permissions().denies(), 0);

    auto auth_object_no_license = memgraph::auth::User::Deserialize(mg_enterprise_no_license_json);
    ASSERT_EQ(auth_object_no_license.username(), "a");
    ASSERT_EQ(memgraph::utils::UUID::arr_t(auth_object_no_license.uuid()), hash);
    ASSERT_TRUE(auth_object_no_license.CheckPasswordExplicit("password"));
    ASSERT_EQ(auth_object_no_license.permissions().grants(), 134217727);
    ASSERT_EQ(auth_object_no_license.permissions().denies(), 0);

    auto auth_object_community = memgraph::auth::User::Deserialize(community_json);
    ASSERT_EQ(auth_object_community.username(), "a");
    ASSERT_EQ(memgraph::utils::UUID::arr_t(auth_object_community.uuid()), hash);
    ASSERT_TRUE(auth_object_community.CheckPasswordExplicit("password"));
    ASSERT_EQ(auth_object_community.permissions().grants(), 134217727);
    ASSERT_EQ(auth_object_community.permissions().denies(), 0);

    auto auth_object_community_resaved = memgraph::auth::User::Deserialize(community_saved_with_license_json);
    ASSERT_EQ(auth_object_community_resaved.username(), "a");
    ASSERT_EQ(memgraph::utils::UUID::arr_t(auth_object_community_resaved.uuid()), hash);
    ASSERT_TRUE(auth_object_community_resaved.CheckPasswordExplicit("password"));
    ASSERT_EQ(auth_object_community_resaved.permissions().grants(), 134217727);
    ASSERT_EQ(auth_object_community_resaved.permissions().denies(), 0);
  }

  // Empty JSON (throw)
  {
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(memgraph::auth::User::Deserialize(nlohmann::json{}), memgraph::auth::AuthException);
  }

  // Missing username (throw)
  {
    auto json = full_json;
    json.erase(kUsername);
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(memgraph::auth::User::Deserialize(json), memgraph::auth::AuthException);
  }

  // Missing permissions (throw)
  {
    auto json = full_json;
    json.erase(kPermissions);
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(memgraph::auth::User::Deserialize(json), memgraph::auth::AuthException);
  }

  // Missing password hash (throw)
  {
    auto json = full_json;
    json.erase(kPasswordHash);
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(memgraph::auth::User::Deserialize(json), memgraph::auth::AuthException);
  }

  // Null password
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

  // Missing hash algo (throw)
  {
    auto json = full_json;
    json[kPasswordHash].erase(kHashAlgo);
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(memgraph::auth::User::Deserialize(json), memgraph::auth::AuthException);
  }

  // Missing hash value (throw)
  {
    auto json = full_json;
    json[kPasswordHash].erase(kPasswordHash);
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_THROW(memgraph::auth::User::Deserialize(json), memgraph::auth::AuthException);
  }

  // Missing UUID ( default to random UUID )
  {
    auto json = full_json;
    json.erase(kUUID);
    memgraph::license::global_license_checker.DisableTesting();
    ASSERT_FALSE(memgraph::license::global_license_checker.IsEnterpriseValidFast());
    ASSERT_NO_THROW(memgraph::auth::User::Deserialize(json));
  }
}

TEST(AuthModule, Reserialization) {
  auto full_json = nlohmann::json::parse(full_json_str);
  auto mg_enterprise_no_license_json = nlohmann::json::parse(mg_enterprise_no_license_json_str);
  auto community_json = nlohmann::json::parse(community_json_str);
  auto community_saved_with_license_json = nlohmann::json::parse(community_saved_with_license_json_str);

  // Re-serialize user
  {
    // Created with license; enterprise fields should be missing
    auto full_object = memgraph::auth::User::Deserialize(full_json);
    EXPECT_EQ(community_json, full_object.Serialize());

    // Created without license, enterprise fields should be missing
    auto no_license_object = memgraph::auth::User::Deserialize(mg_enterprise_no_license_json);
    EXPECT_EQ(community_json, no_license_object.Serialize());

    // Created in community version; should be identical
    auto community_object = memgraph::auth::User::Deserialize(community_json);
    EXPECT_EQ(community_json, community_object.Serialize());

    // Created without license, but save with license, enterprise fields should be missing
    auto save_with_license_object = memgraph::auth::User::Deserialize(community_saved_with_license_json);
    EXPECT_EQ(community_json, save_with_license_object.Serialize());
  }
}

TEST(AuthModule, UserSerialization) {
  memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
  ASSERT_TRUE(memgraph::license::global_license_checker.IsEnterpriseValidFast());

  auto json = nlohmann::json::parse(R"({
          "password_hash":null,
          "permissions":{"denies":0,"grants":0},
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
  memgraph::auth::User user{
      "test_user", memgraph::auth::HashedPassword{memgraph::auth::PasswordHashAlgorithm::SHA256_MULTIPLE, "anything"},
      memgraph::auth::Permissions{/* no permissions */}};
  // Need to update UUID (no way to inject)
  json[kUUID] = user.uuid();
  json[kUsername] = "test_user";
  json[kPasswordHash] = nlohmann::json::object({{"hash_algo", 2}, {"password_hash", "anything"}});
  ASSERT_EQ(json, user.Serialize());

  // User with permissions
  user.GetPermissions().Deny(memgraph::auth::Permission::CONSTRAINT);
  json[kPermissions][kDenies] = user.GetPermissions().denies();
  ASSERT_EQ(json, user.Serialize());
  user.GetPermissions().Grant(memgraph::auth::Permission::DELETE);
  user.GetPermissions().Grant(memgraph::auth::Permission::AUTH);
  json[kPermissions][kGrants] = user.GetPermissions().grants();
  ASSERT_EQ(json, user.Serialize());
}
