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

#include "auth/auth_storage.hpp"
#include "kvstore/kvstore.hpp"
#include "utils/file.hpp"

namespace fs = std::filesystem;
using memgraph::auth::AtomicAuthOverlay;
using memgraph::auth::AuthStorage;

// Each test runs its body against both arms of the adapter and asserts they agree. The overlay arm is flushed
// afterwards, so a passing test also shows the two reach the same durable state.
class AuthStorageTest : public ::testing::Test {
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
      AuthStorage direct{*direct_store_};
      body(direct);
    }
    {
      AtomicAuthOverlay overlay{*overlay_store_};
      AuthStorage buffered{overlay};
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

  fs::path test_folder_{fs::temp_directory_path() / "MG_tests_unit_auth_storage"};
  std::optional<memgraph::kvstore::KVStore> direct_store_;
  std::optional<memgraph::kvstore::KVStore> overlay_store_;
};

TEST_F(AuthStorageTest, PutThenGet) {
  ForBothArms([](AuthStorage &storage) {
    EXPECT_TRUE(storage.Put("user:alice", "data"));
    EXPECT_EQ(storage.Get("user:alice"), "data");
  });
  ExpectSameState();
}

TEST_F(AuthStorageTest, GetMissingKey) {
  ForBothArms([](AuthStorage &storage) { EXPECT_EQ(storage.Get("user:nobody"), std::nullopt); });
  ExpectSameState();
}

TEST_F(AuthStorageTest, Overwrite) {
  ForBothArms([](AuthStorage &storage) {
    EXPECT_TRUE(storage.Put("user:alice", "first"));
    EXPECT_TRUE(storage.Put("user:alice", "second"));
    EXPECT_EQ(storage.Get("user:alice"), "second");
  });
  ExpectSameState();
}

TEST_F(AuthStorageTest, Delete) {
  ForBothArms([](AuthStorage &storage) {
    EXPECT_TRUE(storage.Put("user:alice", "data"));
    EXPECT_TRUE(storage.Delete("user:alice"));
    EXPECT_EQ(storage.Get("user:alice"), std::nullopt);
  });
  ExpectSameState();
}

TEST_F(AuthStorageTest, PutMultiple) {
  ForBothArms([](AuthStorage &storage) {
    EXPECT_TRUE(storage.PutMultiple({{"user:alice", "a"}, {"user:bob", "b"}}));
    EXPECT_EQ(storage.Get("user:alice"), "a");
    EXPECT_EQ(storage.Get("user:bob"), "b");
  });
  ExpectSameState();
}

TEST_F(AuthStorageTest, DeleteMultiple) {
  ForBothArms([](AuthStorage &storage) {
    EXPECT_TRUE(storage.PutMultiple({{"user:alice", "a"}, {"user:bob", "b"}}));
    EXPECT_TRUE(storage.DeleteMultiple({"user:alice", "user:bob"}));
    EXPECT_EQ(storage.Get("user:alice"), std::nullopt);
    EXPECT_EQ(storage.Get("user:bob"), std::nullopt);
  });
  ExpectSameState();
}

TEST_F(AuthStorageTest, PutAndDeleteMultiple) {
  ForBothArms([](AuthStorage &storage) {
    EXPECT_TRUE(storage.Put("user:stale", "old"));
    EXPECT_TRUE(storage.PutAndDeleteMultiple({{"user:alice", "a"}}, {"user:stale"}));
    EXPECT_EQ(storage.Get("user:alice"), "a");
    EXPECT_EQ(storage.Get("user:stale"), std::nullopt);
  });
  ExpectSameState();
}

TEST_F(AuthStorageTest, Size) {
  ForBothArms([](AuthStorage &storage) {
    EXPECT_TRUE(storage.PutMultiple({{"user:alice", "a"}, {"user:bob", "b"}, {"role:admin", "r"}}));
    EXPECT_EQ(storage.Size("user:"), 2);
    EXPECT_EQ(storage.Size("role:"), 1);
  });
  ExpectSameState();
}

TEST_F(AuthStorageTest, ForEachVisitsPrefix) {
  ForBothArms([](AuthStorage &storage) {
    EXPECT_TRUE(storage.PutMultiple({{"user:alice", "a"}, {"user:bob", "b"}, {"role:admin", "r"}}));
    std::map<std::string, std::string> seen;
    storage.ForEach("user:", [&seen](auto const &entry) { seen.emplace(entry); });
    EXPECT_EQ(seen, (std::map<std::string, std::string>{{"user:alice", "a"}, {"user:bob", "b"}}));
  });
  ExpectSameState();
}

TEST_F(AuthStorageTest, AnyOfShortCircuits) {
  ForBothArms([](AuthStorage &storage) {
    EXPECT_TRUE(storage.PutMultiple({{"user:alice", "a"}, {"user:bob", "b"}}));
    int visited = 0;
    EXPECT_TRUE(storage.AnyOf("user:", [&visited](auto const &) {
      ++visited;
      return true;
    }));
    EXPECT_EQ(visited, 1);
    EXPECT_FALSE(storage.AnyOf("user:", [](auto const &) { return false; }));
  });
  ExpectSameState();
}

TEST_F(AuthStorageTest, HasAny) {
  ForBothArms([](AuthStorage &storage) {
    EXPECT_FALSE(storage.HasAny("user:"));
    EXPECT_TRUE(storage.Put("user:alice", "a"));
    EXPECT_TRUE(storage.HasAny("user:"));
  });
  ExpectSameState();
}

TEST_F(AuthStorageTest, BufferedWritesAreNotVisibleUntilFlush) {
  AtomicAuthOverlay overlay{*overlay_store_};
  AuthStorage buffered{overlay};

  EXPECT_TRUE(buffered.Put("user:alice", "data"));
  EXPECT_EQ(buffered.Get("user:alice"), "data");
  EXPECT_EQ(overlay_store_->Get("user:alice"), std::nullopt);

  ASSERT_TRUE(overlay.Flush());
  EXPECT_EQ(overlay_store_->Get("user:alice"), "data");
}
