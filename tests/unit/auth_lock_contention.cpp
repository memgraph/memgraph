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

// Pins auth lock-narrowing: (a) session_long_policy takes no auth lock; (b) up_to_date_policy, CanImpersonate,
// Authenticate and CREATE USER of an existing name take no exclusive lock; (c) CREATE USER is atomic under
// concurrency; (d) legacy SHA256 hash upgrade persists.

#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <future>
#include <latch>
#include <optional>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "auth/auth.hpp"
#include "auth/models.hpp"
#include "auth_test_utils.hpp"
#include "glue/auth_handler.hpp"
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

constexpr auto kBound = std::chrono::seconds(10);

enum class LockMode : uint8_t { kShared, kExclusive };

// Holds an auth lock on a background thread until Release() or destruction.
class LockHolder {
 public:
  LockHolder(mg::auth::SynchedAuth &auth, LockMode mode)
      : future_{std::async(std::launch::async, [this, &auth, mode] {
          if (mode == LockMode::kShared) {
            auto const guard = auth.ReadLock();
            Hold();
          } else {
            auto const guard = auth.Lock();
            Hold();
          }
        })} {
    held_.wait();
  }

  ~LockHolder() { Release(); }

  LockHolder(const LockHolder &) = delete;
  LockHolder &operator=(const LockHolder &) = delete;

  void Release() {
    if (!future_.valid()) return;
    release_.count_down();
    future_.get();
  }

 private:
  void Hold() {
    held_.count_down();
    release_.wait();
  }

  std::latch held_{1};
  std::latch release_{1};
  std::future<void> future_;
};

// Waits (bounded) for every future, then releases the holder so a blocked op can finish before its future's
// destructor joins it.
template <typename... Ts>
bool CompleteWhileHeld(LockHolder &holder, std::future<Ts> &...futures) {
  bool const all_ready = ((futures.wait_for(kBound) == std::future_status::ready) && ...);
  holder.Release();
  return all_ready;
}

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
  mg::glue::AuthQueryHandler auth_handler_{&*auth};

  void SetUp() override {
    mg::utils::EnsureDir(test_folder_);
    mg::license::global_license_checker.EnableTesting();
  }

  void TearDown() override { std::filesystem::remove_all(test_folder_); }

  // The user is persisted by AddUser; the grant is only added when requested.
  mg::glue::QueryUserOrRole MakeUser(const std::string &username, bool grant_match) {
    {
      auto locked = auth->Lock();
      auto user = AddUser(*locked, username);
      EXPECT_TRUE(user.has_value());
      if (grant_match) {
        user->permissions().Grant(mg::auth::Permission::MATCH);
        locked->SaveUser(*user);
      }
    }
    auto stored = auth->ReadLock()->GetUser(username);
    EXPECT_TRUE(stored.has_value());
    return mg::glue::QueryUserOrRole{&*auth, mg::auth::UserOrRole{std::move(*stored)}};
  }
};

}  // namespace

// ReadLock is shared-compatible with an existing ReadLock — up_to_date_policy must complete.
TEST_F(AuthLockContention, IsAuthorizedUpToDatePolicyCompletesUnderReadLock) {
  auto subject = MakeUser("match_user", true);

  LockHolder holder{*auth, LockMode::kShared};
  auto fut = std::async(std::launch::async, [&] {
    return subject.IsAuthorized({AuthQuery::Privilege::MATCH}, std::nullopt, &mg::query::up_to_date_policy);
  });

  ASSERT_TRUE(CompleteWhileHeld(holder, fut)) << "IsAuthorized(up_to_date_policy) blocked under ReadLock";
  EXPECT_TRUE(fut.get());
}

// session_long_policy reads only the cached principal — must complete under an exclusive write lock, including for
// an anonymous session that has no cached principal.
TEST_F(AuthLockContention, SessionLongPolicyIsLockFreeUnderExclusiveLock) {
  auto granted = MakeUser("granted_user", true);
  auto denied = MakeUser("denied_user", false);
  mg::glue::QueryUserOrRole anonymous{&*auth};

  LockHolder holder{*auth, LockMode::kExclusive};
  auto const authorized = [](mg::glue::QueryUserOrRole &subject) {
    return std::async(std::launch::async, [&subject] {
      return subject.IsAuthorized({AuthQuery::Privilege::MATCH}, std::nullopt, &mg::query::session_long_policy);
    });
  };
  auto granted_fut = authorized(granted);
  auto denied_fut = authorized(denied);
  auto anonymous_fut = authorized(anonymous);

  ASSERT_TRUE(CompleteWhileHeld(holder, granted_fut, denied_fut, anonymous_fut))
      << "session_long_policy blocked under exclusive lock";
  EXPECT_TRUE(granted_fut.get());
  EXPECT_FALSE(denied_fut.get());
  EXPECT_TRUE(anonymous_fut.get());
}

// Authenticate takes no exclusive lock on the success and failure paths — must complete under ReadLock.
TEST_F(AuthLockContention, AuthenticateFreeFunctionCompletesUnderReadLock) {
  {
    auto locked = auth->Lock();
    auto user = AddUser(*locked, "alice");
    ASSERT_TRUE(user.has_value());
    user->UpdatePassword("secret");  // bcrypt by default: IsSalted() == true → no upgrade path
    locked->SaveUser(*user);
  }

  LockHolder holder{*auth, LockMode::kShared};
  auto ok_fut = std::async(std::launch::async, [&] { return mg::auth::Authenticate(*auth, "alice", "secret"); });
  auto bad_fut = std::async(std::launch::async, [&] { return mg::auth::Authenticate(*auth, "alice", "wrong"); });

  ASSERT_TRUE(CompleteWhileHeld(holder, ok_fut, bad_fut)) << "Authenticate blocked under ReadLock";
  EXPECT_TRUE(ok_fut.get().has_value());
  EXPECT_FALSE(bad_fut.get().has_value());
}

// CREATE USER of an existing name is decided under the shared lock — no exclusive lock is taken.
TEST_F(AuthLockContention, CreateExistingUserCompletesUnderReadLock) {
  ASSERT_TRUE(auth_handler_.CreateUser("existing", std::nullopt, nullptr).created);

  LockHolder holder{*auth, LockMode::kShared};
  auto fut = std::async(std::launch::async, [&] { return auth_handler_.CreateUser("existing", "password1", nullptr); });

  ASSERT_TRUE(CompleteWhileHeld(holder, fut)) << "CreateUser of an existing name blocked under ReadLock";
  auto const result = fut.get();
  EXPECT_FALSE(result.created);
  EXPECT_FALSE(result.first_user);
}

// Passwords are hashed outside the lock, so racing creators all reach the exclusive phase; only one may win.
TEST_F(AuthLockContention, ConcurrentCreateUserSameNameCreatesOnce) {
  constexpr int kThreads = 8;
  std::latch start{kThreads};
  std::vector<std::future<mg::query::CreateUserResult>> futures;
  futures.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    futures.push_back(std::async(std::launch::async, [&] {
      start.arrive_and_wait();
      return auth_handler_.CreateUser("racer", "password1", nullptr);
    }));
  }

  int created = 0;
  for (auto &f : futures) {
    ASSERT_EQ(f.wait_for(kBound), std::future_status::ready);
    created += f.get().created ? 1 : 0;
  }
  EXPECT_EQ(created, 1);
}

// first_user is decided under the exclusive lock: of N racing creators on an empty auth, exactly one is first.
TEST_F(AuthLockContention, ConcurrentCreateUserDifferentNamesHasOneFirstUser) {
  constexpr int kThreads = 8;
  std::latch start{kThreads};
  std::vector<std::future<mg::query::CreateUserResult>> futures;
  futures.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    futures.push_back(std::async(std::launch::async, [&, name = "user" + std::to_string(i)] {
      start.arrive_and_wait();
      return auth_handler_.CreateUser(name, "password1", nullptr);
    }));
  }

  int created = 0;
  int first = 0;
  for (auto &f : futures) {
    ASSERT_EQ(f.wait_for(kBound), std::future_status::ready);
    auto const result = f.get();
    created += result.created ? 1 : 0;
    first += result.first_user ? 1 : 0;
  }
  EXPECT_EQ(created, kThreads);
  EXPECT_EQ(first, 1);
}

// Authenticate upgrades a legacy unsalted SHA256 hash to salted bcrypt and persists it.
TEST_F(AuthLockContention, LegacyHashUpgradePersists) {
  constexpr std::string_view kUnsaltedSha256 =
      "sha256:d74ff0ee8da3b9806b18c877dbf29bbde50b5bd8e4dad7a3a725000feb82e8f1";
  {
    auto locked = auth->Lock();
    auto user = AddUser(*locked, "bob", std::string{kUnsaltedSha256});
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
// CanImpersonate takes no exclusive lock — must complete while another thread holds ReadLock.
TEST_F(AuthLockContention, CanImpersonateCompletesUnderReadLock) {
  {
    auto locked = auth->Lock();
    auto impersonator = AddUser(*locked, "impersonator");
    ASSERT_TRUE(impersonator.has_value());
    impersonator->permissions().Grant(mg::auth::Permission::IMPERSONATE_USER);
    impersonator->GrantUserImp();
    locked->SaveUser(*impersonator);

    auto target = AddUser(*locked, "target");
    ASSERT_TRUE(target.has_value());
    locked->SaveUser(*target);
  }

  auto stored_imp = auth->ReadLock()->GetUser("impersonator");
  ASSERT_TRUE(stored_imp.has_value());
  mg::glue::QueryUserOrRole subject{&*auth, mg::auth::UserOrRole{std::move(*stored_imp)}};

  LockHolder holder{*auth, LockMode::kShared};
  auto fut =
      std::async(std::launch::async, [&] { return subject.CanImpersonate("target", &mg::query::up_to_date_policy); });

  ASSERT_TRUE(CompleteWhileHeld(holder, fut)) << "CanImpersonate blocked under ReadLock";
  EXPECT_TRUE(fut.get());
}
#endif  // MG_ENTERPRISE
