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

#include <gtest/gtest.h>

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "auth/repository.hpp"
#include "kvstore/kvstore.hpp"
#include "utils/file.hpp"

namespace fs = std::filesystem;
using memgraph::auth::AtomicAuthOverlay;
using memgraph::auth::Repository;

// Each test runs its body against both arms of the adapter and asserts they agree. The overlay arm is flushed
// afterwards, so a passing test also shows the two reach the same durable state.
class RepositoryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    memgraph::utils::EnsureDir(test_folder_);
    direct_store_.emplace(test_folder_ / "direct");
    overlay_store_.emplace(test_folder_ / "overlay");
  }

  void TearDown() override { fs::remove_all(test_folder_); }

  // Runs `body` against the durable store and against an overlay, then flushes the overlay.
  template <typename Fn>
  void ForBothArms(Fn &&body) {
    {
      Repository direct{*direct_store_};
      body(direct);
    }
    {
      AtomicAuthOverlay overlay{*overlay_store_};
      Repository buffered{overlay};
      body(buffered);
      ASSERT_TRUE(overlay.Flush());
    }
  }

  void ExpectSameState(std::string const &prefix = "") {
    std::map<std::string, std::string> direct;
    std::map<std::string, std::string> overlay;
    for (auto it = direct_store_->begin(prefix); it != direct_store_->end(prefix); ++it) direct.emplace(*it);
    for (auto it = overlay_store_->begin(prefix); it != overlay_store_->end(prefix); ++it) overlay.emplace(*it);
    EXPECT_EQ(direct, overlay);
  }

  fs::path test_folder_{fs::temp_directory_path() / "MG_tests_unit_auth_repository"};
  std::optional<memgraph::kvstore::KVStore> direct_store_;
  std::optional<memgraph::kvstore::KVStore> overlay_store_;
};

TEST_F(RepositoryTest, PutThenGet) {
  ForBothArms([](Repository &storage) {
    EXPECT_TRUE(storage.Put("user:alice", "data"));
    EXPECT_EQ(storage.Get("user:alice"), "data");
  });
  ExpectSameState();
}

TEST_F(RepositoryTest, GetMissingKey) {
  ForBothArms([](Repository &storage) { EXPECT_EQ(storage.Get("user:nobody"), std::nullopt); });
  ExpectSameState();
}

TEST_F(RepositoryTest, Overwrite) {
  ForBothArms([](Repository &storage) {
    EXPECT_TRUE(storage.Put("user:alice", "first"));
    EXPECT_TRUE(storage.Put("user:alice", "second"));
    EXPECT_EQ(storage.Get("user:alice"), "second");
  });
  ExpectSameState();
}

TEST_F(RepositoryTest, Delete) {
  ForBothArms([](Repository &storage) {
    EXPECT_TRUE(storage.Put("user:alice", "data"));
    EXPECT_TRUE(storage.Delete("user:alice"));
    EXPECT_EQ(storage.Get("user:alice"), std::nullopt);
  });
  ExpectSameState();
}

TEST_F(RepositoryTest, PutMultiple) {
  ForBothArms([](Repository &storage) {
    EXPECT_TRUE(storage.PutMultiple({{"user:alice", "a"}, {"user:bob", "b"}}));
    EXPECT_EQ(storage.Get("user:alice"), "a");
    EXPECT_EQ(storage.Get("user:bob"), "b");
  });
  ExpectSameState();
}

TEST_F(RepositoryTest, DeleteMultiple) {
  ForBothArms([](Repository &storage) {
    EXPECT_TRUE(storage.PutMultiple({{"user:alice", "a"}, {"user:bob", "b"}}));
    EXPECT_TRUE(storage.DeleteMultiple({"user:alice", "user:bob"}));
    EXPECT_EQ(storage.Get("user:alice"), std::nullopt);
    EXPECT_EQ(storage.Get("user:bob"), std::nullopt);
  });
  ExpectSameState();
}

TEST_F(RepositoryTest, PutAndDeleteMultiple) {
  ForBothArms([](Repository &storage) {
    EXPECT_TRUE(storage.Put("user:stale", "old"));
    EXPECT_TRUE(storage.PutAndDeleteMultiple({{"user:alice", "a"}}, {"user:stale"}));
    EXPECT_EQ(storage.Get("user:alice"), "a");
    EXPECT_EQ(storage.Get("user:stale"), std::nullopt);
  });
  ExpectSameState();
}

TEST_F(RepositoryTest, EntityScanYieldsNamesWithoutPrefix) {
  ForBothArms([](Repository &storage) {
    EXPECT_TRUE(storage.PutMultiple(
        {{Repository::UserKey("alice"), "a"}, {Repository::UserKey("bob"), "b"}, {Repository::RoleKey("admin"), "r"}}));
    std::map<std::string, std::string> seen;
    storage.ForEachUser([&seen](auto const &name, auto const &value) { seen.emplace(name, value); });
    EXPECT_EQ(seen, (std::map<std::string, std::string>{{"alice", "a"}, {"bob", "b"}}));
  });
  ExpectSameState();
}

TEST_F(RepositoryTest, EntityScansDoNotSeeEachOther) {
  ForBothArms([](Repository &storage) {
    EXPECT_TRUE(storage.PutMultiple({{Repository::UserKey("alice"), "a"},
                                     {Repository::RoleLinkKey("alice"), "l"},
                                     {Repository::MtLinkKey("alice"), "m"},
                                     {Repository::ProfileKey("alice"), "p"}}));
    auto count = [&](auto scan) {
      int n = 0;
      scan([&n](auto const &, auto const &) { ++n; });
      return n;
    };
    EXPECT_EQ(count([&](auto &&fn) { storage.ForEachUser(fn); }), 1);
    EXPECT_EQ(count([&](auto &&fn) { storage.ForEachRoleLink(fn); }), 1);
    EXPECT_EQ(count([&](auto &&fn) { storage.ForEachMtLink(fn); }), 1);
    EXPECT_EQ(count([&](auto &&fn) { storage.ForEachProfile(fn); }), 1);
    EXPECT_EQ(count([&](auto &&fn) { storage.ForEachRole(fn); }), 0);
  });
  ExpectSameState();
}

TEST_F(RepositoryTest, HasAnyEntity) {
  ForBothArms([](Repository &storage) {
    EXPECT_FALSE(storage.HasAnyUser());
    EXPECT_FALSE(storage.HasAnyRole());
    EXPECT_TRUE(storage.Put(Repository::UserKey("alice"), "a"));
    EXPECT_TRUE(storage.HasAnyUser());
    EXPECT_FALSE(storage.HasAnyRole());
  });
  ExpectSameState();
}

TEST_F(RepositoryTest, BufferedWritesAreNotVisibleUntilFlush) {
  AtomicAuthOverlay overlay{*overlay_store_};
  Repository buffered{overlay};

  EXPECT_TRUE(buffered.Put("user:alice", "data"));
  EXPECT_EQ(buffered.Get("user:alice"), "data");
  EXPECT_EQ(overlay_store_->Get("user:alice"), std::nullopt);

  ASSERT_TRUE(overlay.Flush());
  EXPECT_EQ(overlay_store_->Get("user:alice"), "data");
}

TEST_F(RepositoryTest, HasAnyDoesNotBindTheTransactionToKeysItNeverRead) {
  // HasAny stops at the first key, so it learns only that the prefix is inhabited. Holding it to every key under
  // the prefix would conflict a transaction against keys it was never obliged to look at.
  overlay_store_->Put("user:one", "1");
  overlay_store_->Put("user:two", "2");

  AtomicAuthOverlay overlay{*overlay_store_};
  Repository repo{overlay};
  ASSERT_TRUE(repo.HasAnyUser());
  repo.Put("user:three", "3");

  EXPECT_TRUE(overlay.Flush());
  EXPECT_TRUE(overlay_store_->Get("user:three").has_value());
}

TEST_F(RepositoryTest, HasAnyStillConflictsWhenEmptinessFlips) {
  // The dependency HasAny does have: an empty prefix gaining a key invalidates whatever the caller concluded.
  // This is the double-superuser race.
  AtomicAuthOverlay overlay{*overlay_store_};
  Repository repo{overlay};
  ASSERT_FALSE(repo.HasAnyUser());
  repo.Put("user:mine", "1");

  overlay_store_->Put("user:theirs", "2");  // another transaction got there first

  EXPECT_FALSE(overlay.Flush());
  EXPECT_FALSE(overlay_store_->Get("user:mine").has_value());
}

TEST_F(RepositoryTest, AFullScanStillDependsOnTheWholeKeySet) {
  // ForEachEntity runs to exhaustion, so it depends on every key; a key appearing under the prefix invalidates it,
  // whatever order it ran in relative to a HasAny on the same prefix.
  overlay_store_->Put("user:one", "1");

  AtomicAuthOverlay overlay{*overlay_store_};
  Repository repo{overlay};
  ASSERT_TRUE(repo.HasAnyUser());
  repo.ForEachUser([](auto &&...) {});
  repo.Put("user:mine", "1");

  overlay_store_->Put("user:appeared", "2");

  EXPECT_FALSE(overlay.Flush());
}

TEST_F(RepositoryTest, AFullScanIsNotWeakenedByALaterHasAny) {
  // The mirror of the test above, and the order that actually happens: SHOW USERS scans the prefix to exhaustion,
  // then CREATE USER calls HasUsers on the same prefix. The short-circuiting scan must not discard what the
  // exhaustive one established, or the transaction commits on a user list that has since changed.
  overlay_store_->Put("user:one", "1");

  AtomicAuthOverlay overlay{*overlay_store_};
  Repository repo{overlay};
  repo.ForEachUser([](auto &&...) {});
  ASSERT_TRUE(repo.HasAnyUser());
  repo.Put("user:mine", "1");

  overlay_store_->Put("user:appeared", "2");

  EXPECT_FALSE(overlay.Flush());
}

TEST_F(RepositoryTest, HasAnyAfterAFullScanStillAllowsAnUnrelatedCommit) {
  // The same ordering must not start over-conflicting either: with the key set unchanged, both scans' premises
  // still hold and the transaction commits.
  overlay_store_->Put("user:one", "1");
  overlay_store_->Put("user:two", "2");

  AtomicAuthOverlay overlay{*overlay_store_};
  Repository repo{overlay};
  repo.ForEachUser([](auto &&...) {});
  ASSERT_TRUE(repo.HasAnyUser());
  repo.Put("user:mine", "1");

  EXPECT_TRUE(overlay.Flush());
  EXPECT_TRUE(overlay_store_->Get("user:mine").has_value());
}
