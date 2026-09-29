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

// Pins auth lock-narrowing: (a) session_long_policy is lock-free; (b) up_to_date_policy and
// CanImpersonate take ReadLock, not the exclusive lock; (c) Authenticate takes ReadLock for
// GetUser, bcrypt runs lock-free; (d) legacy SHA256 hash upgrade persists.

#include <chrono>
#include <future>
#include <latch>
#include <thread>

#include <gtest/gtest.h>

#include "auth/auth.hpp"
#include "auth/crypto.hpp"
#include "auth/models.hpp"
#include "glue/auth_global.hpp"
#include "glue/query_user.hpp"
#include "license/license.hpp"
#include "query/frontend/ast/query/auth_query.hpp"
#include "query/query_user.hpp"
#include "utils/file.hpp"

#ifdef MG_ENTERPRISE
#include "utils/resource_monitoring.hpp"
#endif

namespace mg = memgraph;
using mg::query::AuthQuery;

namespace {

class AuthLockContention : public ::testing::Test {
 protected:
  std::filesystem::path test_folder_{std::filesystem::temp_directory_path() / "MG_tests_unit_auth_lock_contention"};
  std::filesystem::path auth_dir_ =
      test_folder_ / ("auth_lock_contention_" + std::to_string(static_cast<int>(getpid())));

#ifdef MG_ENTERPRISE
  mg::utils::ResourceMonitoring resources_{};
#endif

  // SynchedAuth wraps Auth in a WritePrioritizedRWLock — ReadLock calls below are shared-compatible.
  std::optional<mg::auth::SynchedAuth> auth{std::in_place,
                                            auth_dir_,
                                            mg::auth::Auth::Config{}
#ifdef MG_ENTERPRISE
                                            ,
                                            &resources_
#endif
  };

  void SetUp() override {
    mg::utils::EnsureDir(test_folder_);
    mg::license::global_license_checker.EnableTesting();
  }

  void TearDown() override { std::filesystem::remove_all(test_folder_); }

  mg::glue::QueryUserOrRole MakeGrantedUser(const std::string &username) {
    {
      auto locked = auth->Lock();
      auto user = locked->AddUser(username);
      EXPECT_TRUE(user.has_value());
      user->permissions().Grant(mg::auth::Permission::MATCH);
      locked->SaveUser(*user);
    }
    auto stored = auth->ReadLock()->GetUser(username);
    EXPECT_TRUE(stored.has_value());
    return mg::glue::QueryUserOrRole{&*auth, mg::auth::UserOrRole{std::move(*stored)}};
  }

  mg::glue::QueryUserOrRole MakeDeniedUser(const std::string &username) {
    {
      auto locked = auth->Lock();
      auto user = locked->AddUser(username);
      EXPECT_TRUE(user.has_value());
      // No grants: AddUser already persisted the user with empty permissions.
    }
    auto stored = auth->ReadLock()->GetUser(username);
    EXPECT_TRUE(stored.has_value());
    return mg::glue::QueryUserOrRole{&*auth, mg::auth::UserOrRole{std::move(*stored)}};
  }
};

}  // namespace

// ReadLock is shared-compatible with an existing ReadLock — up_to_date_policy must complete.
TEST_F(AuthLockContention, IsAuthorizedUpToDatePolicyCompletesUnderReadLock) {
  auto subject = MakeGrantedUser("match_user");

  std::latch lock_held{1};
  std::latch op_done{1};

  auto holder = std::async(std::launch::async, [&] {
    auto guard = auth->ReadLock();
    lock_held.count_down();
    op_done.wait();
  });

  lock_held.wait();

  auto op_fut = std::async(std::launch::async, [&] {
    return subject.IsAuthorized({AuthQuery::Privilege::MATCH}, std::nullopt, &mg::query::up_to_date_policy);
  });

  auto status = op_fut.wait_for(std::chrono::seconds(10));
  op_done.count_down();  // always release the holder so its latch is never destroyed while blocked
  holder.get();

  ASSERT_EQ(status, std::future_status::ready) << "IsAuthorized(up_to_date_policy) blocked under ReadLock";
  EXPECT_TRUE(op_fut.get());
}

// session_long_policy reads only the cached principal — must complete under an exclusive write lock.
TEST_F(AuthLockContention, SessionLongPolicyIsLockFreeUnderExclusiveLock) {
  auto granted = MakeGrantedUser("granted_user");
  auto denied = MakeDeniedUser("denied_user");

  std::latch lock_held{1};
  std::latch op_done{1};

  auto holder = std::async(std::launch::async, [&] {
    auto guard = auth->Lock();
    lock_held.count_down();
    op_done.wait();
  });

  lock_held.wait();

  auto granted_fut = std::async(std::launch::async, [&] {
    return granted.IsAuthorized({AuthQuery::Privilege::MATCH}, std::nullopt, &mg::query::session_long_policy);
  });
  auto denied_fut = std::async(std::launch::async, [&] {
    return denied.IsAuthorized({AuthQuery::Privilege::MATCH}, std::nullopt, &mg::query::session_long_policy);
  });

  auto gs = granted_fut.wait_for(std::chrono::seconds(10));
  auto ds = denied_fut.wait_for(std::chrono::seconds(10));

  op_done.count_down();
  holder.get();

  ASSERT_EQ(gs, std::future_status::ready) << "session_long_policy (granted) blocked under exclusive lock";
  ASSERT_EQ(ds, std::future_status::ready) << "session_long_policy (denied) blocked under exclusive lock";
  EXPECT_TRUE(granted_fut.get());
  EXPECT_FALSE(denied_fut.get());
}

// Authenticate takes ReadLock for GetUser; bcrypt runs lock-free — must complete under ReadLock.
TEST_F(AuthLockContention, AuthenticateFreeFunctionCompletesUnderReadLock) {
  {
    auto locked = auth->Lock();
    auto user = locked->AddUser("alice");
    ASSERT_TRUE(user.has_value());
    user->UpdatePassword("secret");  // bcrypt by default: IsSalted() == true → no upgrade path
    locked->SaveUser(*user);
  }

  std::latch lock_held{1};
  std::latch op_done{1};

  auto holder = std::async(std::launch::async, [&] {
    auto guard = auth->ReadLock();
    lock_held.count_down();
    op_done.wait();
  });

  lock_held.wait();

  auto ok_fut = std::async(std::launch::async, [&] { return mg::auth::Authenticate(*auth, "alice", "secret"); });
  auto bad_fut = std::async(std::launch::async, [&] { return mg::auth::Authenticate(*auth, "alice", "wrong"); });

  auto ok_s = ok_fut.wait_for(std::chrono::seconds(10));
  auto bad_s = bad_fut.wait_for(std::chrono::seconds(10));

  op_done.count_down();
  holder.get();

  ASSERT_EQ(ok_s, std::future_status::ready) << "Authenticate (correct pw) blocked under ReadLock";
  ASSERT_EQ(bad_s, std::future_status::ready) << "Authenticate (wrong pw) blocked under ReadLock";
  EXPECT_TRUE(ok_fut.get().has_value());
  EXPECT_FALSE(bad_fut.get().has_value());
}

// Authenticate upgrades a legacy unsalted SHA256 hash to salted bcrypt and persists it.
TEST_F(AuthLockContention, LegacyHashUpgradePersists) {
  constexpr std::string_view kUnsaltedSha256 =
      "sha256:d74ff0ee8da3b9806b18c877dbf29bbde50b5bd8e4dad7a3a725000feb82e8f1";
  {
    auto locked = auth->Lock();
    auto user = locked->AddUser("bob", std::string{kUnsaltedSha256});
    ASSERT_TRUE(user.has_value());
  }

  {
    auto before = auth->ReadLock()->GetUser("bob");
    ASSERT_TRUE(before.has_value());
    ASSERT_TRUE(before->password_hash().has_value());
    ASSERT_FALSE(before->password_hash()->IsSalted()) << "SHA256 hash must start unsalted";
  }

  auto result = mg::auth::Authenticate(*auth, "bob", "pass");
  ASSERT_TRUE(result.has_value()) << "Authenticate must succeed for the correct password";

  auto after = auth->ReadLock()->GetUser("bob");
  ASSERT_TRUE(after.has_value());
  ASSERT_TRUE(after->password_hash().has_value());
  EXPECT_TRUE(after->password_hash()->IsSalted()) << "stored hash must be salted after upgrade";
}

#ifdef MG_ENTERPRISE
// CanImpersonate takes ReadLock internally — must complete while another thread holds ReadLock.
TEST_F(AuthLockContention, CanImpersonateCompletesUnderReadLock) {
  {
    auto locked = auth->Lock();
    auto impersonator = locked->AddUser("impersonator");
    ASSERT_TRUE(impersonator.has_value());
    impersonator->permissions().Grant(mg::auth::Permission::IMPERSONATE_USER);
    impersonator->GrantUserImp();
    locked->SaveUser(*impersonator);

    auto target = locked->AddUser("target");
    ASSERT_TRUE(target.has_value());
    locked->SaveUser(*target);
  }

  auto stored_imp = auth->ReadLock()->GetUser("impersonator");
  ASSERT_TRUE(stored_imp.has_value());
  mg::glue::QueryUserOrRole subject{&*auth, mg::auth::UserOrRole{std::move(*stored_imp)}};

  std::latch lock_held{1};
  std::latch op_done{1};

  auto holder = std::async(std::launch::async, [&] {
    auto guard = auth->ReadLock();
    lock_held.count_down();
    op_done.wait();
  });

  lock_held.wait();

  auto impersonate_fut =
      std::async(std::launch::async, [&] { return subject.CanImpersonate("target", &mg::query::up_to_date_policy); });

  auto status = impersonate_fut.wait_for(std::chrono::seconds(10));
  op_done.count_down();
  holder.get();

  ASSERT_EQ(status, std::future_status::ready) << "CanImpersonate blocked under ReadLock";
  EXPECT_TRUE(impersonate_fut.get());
}
#endif  // MG_ENTERPRISE
